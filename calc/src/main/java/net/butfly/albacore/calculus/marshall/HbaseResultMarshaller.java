package net.butfly.albacore.calculus.marshall;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.jongo.marshall.jackson.bson4jackson.MongoBsonFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.jcabi.log.Logger;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.FunctorConfig.Detail;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource.HbaseDataSource;
import net.butfly.albacore.calculus.datasource.ColumnFamily;
import net.butfly.albacore.utils.Reflections;

public class HbaseResultMarshaller implements Marshaller<Result, ImmutableBytesWritable> {
	private static final long serialVersionUID = -4529825710243214685L;
	private static ObjectMapper mapper = new ObjectMapper(new MongoBsonFactory());

	@Override
	public <T extends Functor<T>> T unmarshall(Result from, Class<T> to) {
		String dcf = to.isAnnotationPresent(ColumnFamily.class) ? to.getAnnotation(ColumnFamily.class).value() : null;
		T t = Reflections.construct(to);
		for (Field f : Reflections.getDeclaredFields(to)) {
			String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
					: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
			String colfamily = f.isAnnotationPresent(ColumnFamily.class) ? f.getAnnotation(ColumnFamily.class).value() : dcf;
			if (colfamily == null)
				throw new IllegalArgumentException("Column family is not defined on class " + to.toString() + ", field " + f.getName());
			try {
				f.set(t, mapper.readValue(
						CellUtil.cloneValue(from.getColumnLatestCell(Text.encode(colfamily).array(), Text.encode(colname).array())),
						f.getType()));
			} catch (Exception e) {
				Logger.error(HbaseResultMarshaller.class,
						"Parse of hbase result failure on  class " + to.toString() + ", field " + f.getName());
			}
		}
		return t;
	}

	@Override
	public <T extends Functor<T>> Result marshall(T from) {
		throw new UnsupportedOperationException("Hbase marshall / write not supported.");
	}

	@Override
	public String unmarshallId(ImmutableBytesWritable id) {
		try {
			return Text.decode(id.get());
		} catch (CharacterCodingException e) {
			Logger.error(HbaseResultMarshaller.class, "ImmutableBytesWritable unmarshall failure.", e);
			return null;
		}
	}

	@Override
	public ImmutableBytesWritable marshallId(String id) {
		try {
			return new ImmutableBytesWritable(Text.encode(id).array());
		} catch (CharacterCodingException e) {
			Logger.error(HbaseResultMarshaller.class, "ImmutableBytesWritable marshall failure.", e);
			return null;
		}
	}

	@Override
	public <F extends Functor<F>> void confirm(Class<F> functor, CalculatorDataSource ds, Detail detail) {
		try {
			TableName ht = TableName.valueOf(detail.hbaseTable);
			Admin a = ((HbaseDataSource) ds).getHconn().getAdmin();
			if (a.tableExists(ht)) return;
			Set<String> families = new HashSet<>();
			Set<String> columns = new HashSet<>();
			String dcf = functor.isAnnotationPresent(ColumnFamily.class) ? functor.getAnnotation(ColumnFamily.class).value() : null;
			families.add(dcf);
			for (Field f : Reflections.getDeclaredFields(functor)) {
				String colname = f.isAnnotationPresent(JsonProperty.class) ? f.getAnnotation(JsonProperty.class).value()
						: CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, f.getName());
				String colfamily = f.isAnnotationPresent(ColumnFamily.class) ? f.getAnnotation(ColumnFamily.class).value() : dcf;
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
		} catch (IOException e) {
			throw new IllegalArgumentException("Hbase verify failure on server: " + ds.toString() + ", class: " + functor.toString());
		}
	}
}
