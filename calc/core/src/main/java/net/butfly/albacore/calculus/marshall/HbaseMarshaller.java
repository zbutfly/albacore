package net.butfly.albacore.calculus.marshall;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CaseFormat;
import com.google.common.base.Defaults;
import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.Calculator;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.HbaseDataSource;
import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.datasource.HbaseColumnFamily;
import net.butfly.albacore.calculus.utils.Reflections;

public class HbaseMarshaller extends Marshaller<Result, ImmutableBytesWritable> {
	private static final long serialVersionUID = -4529825710243214685L;
	public static final String SCAN_LIMIT = "hbase.calculus.limit";
	public static final String SCAN_OFFSET = "hbase.calculus.limit";

	private String[] rows(Result result) {
		List<String> rows = new ArrayList<>();
		for (Cell c : result.rawCells())
			rows.add(Bytes.toString(CellUtil.cloneQualifier(c)));
		return rows.toArray(new String[rows.size()]);
	}

	@Override
	public <T extends Factor<T>> T unmarshall(Result from, Class<T> to) {
		if (null == from) return null;
		String dcf = to.isAnnotationPresent(HbaseColumnFamily.class) ? to.getAnnotation(HbaseColumnFamily.class).value() : null;
		T t;
		try {
			t = to.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		for (Field f : Reflections.getDeclaredFields(to)) {
			String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
					: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
			String colfamily = f.isAnnotationPresent(HbaseColumnFamily.class) ? f.getAnnotation(HbaseColumnFamily.class).value() : dcf;
			if (colfamily == null)
				throw new IllegalArgumentException("Column family is not defined on " + to.toString() + ", field " + f.getName());
			try {
				Cell cell = from.getColumnLatestCell(Text.encode(colfamily).array(), Text.encode(colname).array());
				if (cell != null) Reflections.set(t, f, fromBytes(f.getType(), CellUtil.cloneValue(cell)));
				else if (Calculator.debug && logger.isTraceEnabled())
					logger.trace("Rows of table for [" + to.toString() + "]: " + Joiner.on(',').join(rows(from)));
			} catch (Exception e) {
				logger.error("Parse of hbase result failure on " + to.toString() + ", field " + f.getName(), e);
			}
		}
		return t;

	}

	@Override
	public <T extends Factor<T>> Result marshall(T from) {
		throw new UnsupportedOperationException("Hbase marshall / write not supported.");
	}

	@Override
	public String unmarshallId(ImmutableBytesWritable id) {
		if (null == id) return null;
		return Bytes.toString(id.get());
	}

	@Override
	public ImmutableBytesWritable marshallId(String id) {
		if (null == id) return null;
		return new ImmutableBytesWritable(Bytes.toBytes(id));
	}

	@Override
	public <F extends Factor<F>> boolean confirm(Class<F> factor, DataSource ds, Detail detail) {
		try {
			TableName ht = TableName.valueOf(detail.hbaseTable);
			Admin a = ((HbaseDataSource) ds).getHconn().getAdmin();
			if (a.tableExists(ht)) return true;
			Set<String> families = new HashSet<>();
			Set<String> columns = new HashSet<>();
			String dcf = factor.isAnnotationPresent(HbaseColumnFamily.class) ? factor.getAnnotation(HbaseColumnFamily.class).value()
					: null;
			families.add(dcf);
			for (Field f : Reflections.getDeclaredFields(factor)) {
				String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
						: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
				String colfamily = f.isAnnotationPresent(HbaseColumnFamily.class) ? f.getAnnotation(HbaseColumnFamily.class).value() : dcf;
				families.add(colfamily);
				columns.add(colfamily + ":" + colname);
			}
			HTableDescriptor td = new HTableDescriptor(ht);
			for (String fn : families) {
				HColumnDescriptor fd = new HColumnDescriptor(fn);
				td.addFamily(fd);
			}
			a.createTable(td);
			a.disableTable(ht);
			for (String col : columns)
				a.addColumn(ht, new HColumnDescriptor(col));
			a.enableTable(ht);
			return true;
		} catch (IOException e) {
			logger.error("Failure confirm data source: " + factor.getName() + " => " + ds.toString() + " => " + detail.toString());
			return false;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <R> R fromBytes(Class<R> type, byte[] value) {
		if (null == value || value.length == 0) return null;
		if (Reflections.isAny(type, CharSequence.class)) return (R) Bytes.toString(value);
		if (Reflections.isAny(type, long.class, Long.class)) return (R) (Long) Bytes.toLong(value);
		if (Reflections.isAny(type, byte.class, Byte.class)) return (R) (value != null && value.length > 0 ? value[0] : null);
		if (Reflections.isAny(type, double.class, Double.class)) return (R) (Double) Bytes.toDouble(value);
		if (Reflections.isAny(type, float.class, Float.class)) return (R) (Float) Bytes.toFloat(value);
		if (Reflections.isAny(type, boolean.class, Boolean.class)) return (R) (Boolean) Bytes.toBoolean(value);
		if (Reflections.isAny(type, int.class, Integer.class)) return (R) (Integer) Bytes.toInt(value);
		if (type.isArray()) {
			byte[][] v = Bytes.toByteArrays(value);
			Object[] r = (Object[]) Array.newInstance(type.getComponentType(), v.length);
			for (int i = 0; i < v.length; i++)
				r[i] = fromBytes(type.getComponentType(), v[i]);
			return (R) r;
		}
		if (Reflections.isAny(type, Collection.class)) {
			byte[][] v = Bytes.toByteArrays(value);
			Collection r = (Collection) Defaults.defaultValue(type);
			Class<?> t = Reflections.resolveGenericParameter(type, Collection.class, "E");
			for (int i = 0; i < v.length; i++)
				r.add(fromBytes(t, v[i]));
			return (R) r;
		}
		throw new UnsupportedOperationException("Not supportted marshall: " + type.toString());
	}

	public static <R> byte[] toBytes(Class<R> type, R value) {
		if (null == value) return null;
		if (Reflections.isAny(type, CharSequence.class)) return Bytes.toBytes(((CharSequence) value).toString());
		if (Reflections.isAny(type, long.class, Long.class)) return Bytes.toBytes((Long) value);
		if (Reflections.isAny(type, byte.class, Byte.class)) return (new byte[] { (Byte) value });
		if (Reflections.isAny(type, double.class, Double.class)) return Bytes.toBytes((Double) value);
		if (Reflections.isAny(type, float.class, Float.class)) return Bytes.toBytes((Float) value);
		if (Reflections.isAny(type, boolean.class, Boolean.class)) return Bytes.toBytes((Boolean) value);
		if (Reflections.isAny(type, int.class, Integer.class)) return Bytes.toBytes((Integer) value);
		throw new UnsupportedOperationException("Not supportted marshall: " + type.toString());
	}

	@SuppressWarnings("unchecked")
	public static <R> byte[][] toByteArray(Class<R> type, R[] value) {
		if (null == value) return null;
		byte[][] r = new byte[value.length][];
		for (int i = 0; i < value.length; i++)
			r[i] = toBytes((Class<R>) type.getComponentType(), value[i]);
		return r;
	}
}
