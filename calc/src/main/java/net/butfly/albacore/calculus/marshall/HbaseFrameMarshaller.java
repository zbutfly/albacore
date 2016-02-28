package net.butfly.albacore.calculus.marshall;

import org.apache.spark.sql.Row;

import net.butfly.albacore.calculus.Functor;

public class HbaseFrameMarshaller implements Marshaller<Row> {
	@Override
	public <T extends Functor<T>> T unmarshall(Row from, Class<Functor<T>> to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Functor<T>> Row marshall(T from) {
		// TODO Auto-generated method stub
		return null;
	}
}
