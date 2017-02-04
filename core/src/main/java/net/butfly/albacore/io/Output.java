package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Output<V> extends Openable, Sizable {
	@Override
	default long size() {
		return 0;
	}

	/**
	 * basic, none blocking writing.
	 * 
	 * @param item
	 * @return
	 */
	boolean enqueue(V item, boolean block);

	default long enqueue(Stream<V> items) {
		Stream<V> s = items.filter(t -> t != null);
		if (!Concurrents.waitSleep(() -> full())) return 0;
		s.forEach(t -> enqueue(t, true));
		return s.count();
	}

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		return priors(Collections.convAs(conv));
	}

	default <V0> Output<V0> priors(Converter<List<V0>, List<V>> conv) {
		return new OutputPriorHandler<>(this, conv).proxy(Output.class);
	}

	final class OutputPriorHandler<V0, V> extends Namedly implements InvocationHandler {
		private final Output<V> output;
		private final Converter<List<V0>, List<V>> conv;

		public OutputPriorHandler(Output<V> output, Converter<List<V0>, List<V>> conv) {
			super(output.name() + "Prior");
			this.output = output;
			this.conv = conv;
		}

		@SuppressWarnings({ "unchecked" })
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			switch (method.getName()) {
			case "name":
				if (null == args || args.length == 0) return name;
				break;
			case "enqueue":
				switch (args.length) {
				case 2:
					return output.enqueue(conv.apply(Arrays.asList((V0) args[0])).get(0), (Boolean) args[1]);
				// case 1:
				// if (List.class.isAssignableFrom(args[0].getClass())) return
				// output.enqueueList(conv.apply((List<V0>) args[0]));
				// else return output.enqueueOne(conv.apply(Arrays.asList((V0)
				// args[0])).get(0));
				}
				break;
			case "prior":
				if (args.length == 1) return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy, //
						Collections.convAs((Converter<V0, V>) args[0])), Output.class);
				break;
			case "priors":
				if (args.length == 1) return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy,
						(Converter<List<V0>, List<V>>) args[0]), Output.class);
				break;
			}
			return method.invoke(output, args);
		}
	}
}
