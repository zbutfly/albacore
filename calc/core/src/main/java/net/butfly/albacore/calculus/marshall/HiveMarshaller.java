package net.butfly.albacore.calculus.marshall;

import java.lang.reflect.Field;

import org.apache.spark.sql.Row;

import net.butfly.albacore.calculus.utils.Reflections;

public class HiveMarshaller extends Marshaller<Object, Row, Row> {
	private static final long serialVersionUID = -4529825710243214685L;

	@Override
	public <T> T unmarshall(Row row, Class<T> to) {
		if (null == row) return null;
		T t;
		try {
			t = to.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		for (Field f : Reflections.getDeclaredFields(to))
			Reflections.set(t, f, row.get(row.fieldIndex(parseQualifier(f))));
		return t;
	}

	@Override
	public <T> Row marshall(T from) {
		// TODO
		throw new UnsupportedOperationException("Hbase marshall / write not supported.");
	}

	@Override
	public Row marshallId(Object id) {
		// TODO Auto-generated method stub
		return super.marshallId(id);
	}
}
