package net.butfly.albacore.serder;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Defaults;
import com.google.common.base.Joiner;
import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.modifier.HbaseColumnFamily;
import net.butfly.albacore.utils.Reflections;

public class HbaseResultSerder extends BeanSerderBase<Result> implements BeanSerder<Result> {
	private static final long serialVersionUID = 1152380944308233135L;
	private static final Logger logger = LoggerFactory.getLogger(HbaseResultSerder.class);

	public HbaseResultSerder() {
		super();
	}

	@Override
	public <T> Result ser(T src) {
		throw new UnsupportedOperationException("Hbase marshall / write not supported.");
	}

	@Override
	public <T> T der(Result from, TypeToken<T> to) {
		if (null == from) return null;
		@SuppressWarnings("unchecked")
		Class<T> type = (Class<T>) to.getRawType();
		T t = Reflections.construct(type);
		for (Field f : Reflections.getDeclaredFields(type)) {
			String[] qulifier = mapping(f).split(":");
			try {
				Cell cell = from.getColumnLatestCell(Text.encode(qulifier[0]).array(), Text.encode(qulifier[1]).array());
				if (cell != null) Reflections.set(t, f, fromBytes(f.getType(), CellUtil.cloneValue(cell)));
				else if (logger.isTraceEnabled()) logger.trace("Rows of table for [" + to.toString() + "]: " + Joiner.on(',').join(rows(
						from)));
			} catch (Exception e) {
				logger.error("Parse of hbase result failure on " + to.toString() + ", field " + f.getName(), e);
			}
		}
		return t;
	}

	private String[] rows(Result result) {
		List<String> rows = new ArrayList<>();
		for (Cell c : result.rawCells())
			rows.add(Bytes.toString(CellUtil.cloneQualifier(c)));
		return rows.toArray(new String[rows.size()]);
	}

	public final String mapping(Field f) {
		Reflections.noneNull("", f);
		String col = mapping(f);
		Class<?> to = f.getDeclaringClass();
		// XXX: field in parent class could not found annotation on sub-class.
		String family = f.isAnnotationPresent(HbaseColumnFamily.class) ? f.getAnnotation(HbaseColumnFamily.class).value()
				: (to.isAnnotationPresent(HbaseColumnFamily.class) ? to.getAnnotation(HbaseColumnFamily.class).value()
						: HbaseColumnFamily.DEFAULT_COLUMN_FAMILY);
		if (family == null) throw new IllegalArgumentException("Column family is not defined on " + to.toString() + ", field " + f
				.getName());
		return family + ":" + col;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <R> R fromBytes(Class<R> type, byte[] value) {
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

	private static <R> byte[] toBytes(Class<R> type, R value) {
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
	private static <R> byte[][] toByteArray(Class<R> type, R[] value) {
		if (null == value) return null;
		byte[][] r = new byte[value.length][];
		for (int i = 0; i < value.length; i++)
			r[i] = toBytes((Class<R>) type.getComponentType(), value[i]);
		return r;
	}
}
