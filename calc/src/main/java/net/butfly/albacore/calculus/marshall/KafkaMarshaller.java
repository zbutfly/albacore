package net.butfly.albacore.calculus.marshall;

import com.jcabi.log.Logger;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.FunctorConfig.Detail;
import net.butfly.albacore.calculus.datasource.CalculatorDataSource;

public class KafkaMarshaller implements Marshaller<String, String> {
	private static final long serialVersionUID = -4471098188111221100L;

	@Override
	public String unmarshallId(String id) {
		Logger.trace(KafkaMarshaller.class, "Unmarshall id: " + id);
		return null;
	}

	@Override
	public <T extends Functor<T>> T unmarshall(String from, Class<T> to) {
		Logger.trace(KafkaMarshaller.class, "Unmarshall data (" + to.toString() + "): " + from);
		return null;
	}

	@Override
	public String marshallId(String id) {
		Logger.trace(KafkaMarshaller.class, "Marshall id: " + id);
		return null;
	}

	@Override
	public <T extends Functor<T>> String marshall(T from) {
		Logger.trace(KafkaMarshaller.class, "Marshall data (" + from.getClass().toString() + "): " + from.toString());
		return null;
	}

	@Override
	public <F extends Functor<F>> void confirm(Class<F> functor, CalculatorDataSource ds, Detail detail) {
		throw new UnsupportedOperationException();
	}
}
