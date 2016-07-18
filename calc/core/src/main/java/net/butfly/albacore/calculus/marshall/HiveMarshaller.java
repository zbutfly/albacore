package net.butfly.albacore.calculus.marshall;

import org.apache.spark.sql.Row;

public class HiveMarshaller extends Marshaller<Object, Object, Row> {
	private static final long serialVersionUID = -4529825710243214685L;

	@Override
	public <T> T unmarshall(Row from, Class<T> to) {
		// TODO
		return null;
	}

	@Override
	public <T> Row marshall(T from) {
		// TODO
		throw new UnsupportedOperationException("Hbase marshall / write not supported.");
	}
}
