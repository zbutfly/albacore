package net.butfly.albacore.calculus.marshall;

import net.butfly.albacore.calculus.CalculatorConfig;
import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.FunctorConfig;

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

	@Override
	public <F extends Functor<F>> void confirm(Class<F> functor, FunctorConfig config, CalculatorConfig globalConfig) {}
}
