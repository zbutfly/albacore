package net.butfly.albacore.calculus.marshall;

import org.bson.BSONObject;

import net.butfly.albacore.calculus.Functor;

public class MongodbMarshaller implements Marshaller<BSONObject> {
	@Override
	public <T extends Functor<T>> T unmarshall(BSONObject from, Class<Functor<T>> to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Functor<T>> BSONObject marshall(T from) {
		// TODO Auto-generated method stub
		return null;
	}
}
