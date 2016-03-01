package net.butfly.albacore.calculus.marshall;

import net.butfly.albacore.calculus.Functor;

public class KafkaMarshaller implements Marshaller<String, String> {
	@Override
	public String unmarshallId(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Functor<T>> T unmarshall(String from, Class<T> to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String marshallId(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Functor<T>> String marshall(T from) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public <F extends Functor> void confirm(Class<F> functor) {
		// TODO Auto-generated method stub

	}
}
