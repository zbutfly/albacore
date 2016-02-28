package net.butfly.albacore.calculus.marshall;

import org.apache.hadoop.hbase.client.Result;

import net.butfly.albacore.calculus.Functor;

public class HbaseResultMarshaller implements Marshaller<Result> {
	@Override
	public <T extends Functor<T>> T unmarshall(Result from, Class<Functor<T>> to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Functor<T>> Result marshall(T from) {
		// TODO Auto-generated method stub
		return null;
	}
}
