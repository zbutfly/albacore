package net.butfly.albacore.calculus.marshall;

import java.lang.reflect.Field;

import org.apache.spark.sql.Row;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.utils.Reflections;

public class RowMarshaller extends Marshaller<Object, Row, Row> {
	private static final long serialVersionUID = -4529825710243214685L;
	// public static RowMarshaller DEFAULT_WITH;

	public RowMarshaller(Func<String, String> mapping) {
		super(mapping);
	}

	@Override
	public <T> T unmarshall(Row row, Class<T> to) {
		if (null == row) return null;
		T t = Reflections.construct(to);
		for (Field f : Reflections.getDeclaredFields(to))
			try {
				Reflections.set(t, f, row.get(row.fieldIndex(parseQualifier(f))));
			} catch (Exception ex) {}
		return t;
	}

	@Override
	public <T> Row marshall(T from) {
		throw new UnsupportedOperationException("Hbase marshall / write not supported.");
	}

	@Override
	public Row marshallId(Object id) {
		return super.marshallId(id);
	}

	public <F extends Factor<F>> void colsAsFields(StringBuilder hql, Class<F> factor) {
		for (Field f : Reflections.getDeclaredFields(factor))
			if (Marshaller.enableDB(f)) hql.append(parseQualifier(f)).append(" as ").append(f.getName()).append(", ");
		if (hql.charAt(hql.length() - 2) == ',' && hql.charAt(hql.length() - 1) == ' ') hql.deleteCharAt(hql.length() - 2);
	}
}
