package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;
import net.butfly.albacore.utils.Collections;

public interface Input<V> extends Openable, Sizable {
	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	/**
	 * basic, none blocking reading.
	 * 
	 * @return null on empty
	 */
	V dequeue(boolean block);

	default Stream<V> dequeue(long batchSize) {
		return IOStreaming.batch(() -> dequeue(false), batchSize).parallelStream().filter(t -> t != null);
	}

	default <V1> Input<V1> then(Converter<V, V1> conv) {
		return thens(Collections.convAs(conv));
	}

	default <V1> Input<V1> thens(Converter<List<V>, List<V1>> conv) {
		return new InputThenHandler<>(this, conv).proxy(Input.class);
	}

	final class InputThenHandler<V, V1> extends Namedly implements InvocationHandler {
		private final Input<V> input;
		private final Converter<List<V>, List<V1>> conv;

		public InputThenHandler(Input<V> input, Converter<List<V>, List<V1>> conv) {
			super(input.name() + "Then");
			this.input = input;
			this.conv = conv;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			switch (method.getName()) {
			case "name":
				if (null == args || args.length == 0) return name;
				break;
			case "dequeue":
				// if (null == args || args.length == 0) return
				// conv.apply(Arrays.asList(input.dequeueOne())).get(0);
				if (args.length == 1) {
					if (Number.class.isAssignableFrom(args[0].getClass())) return input.dequeue(((Number) args[0]).longValue()).map(
							item -> conv.apply(Arrays.asList(item))).collect(Collectors.toList()); // XXX:!!!
					if (Boolean.class.isAssignableFrom(args[0].getClass())) return conv.apply(Arrays.asList(input.dequeue(
							((Boolean) args[0]).booleanValue()))).get(0);
				}
				break;
			case "then":
				if (args.length == 1) return InvocationHandler.proxy(new InputThenHandler<>((Input<V>) proxy, //
						Collections.convAs((Converter<V, V1>) args[0])), Input.class);
				break;
			case "thens":
				if (args.length == 1) return InvocationHandler.proxy(new InputThenHandler<>((Input<V>) proxy,
						(Converter<List<V>, List<V1>>) args[0]), Input.class);
				break;
			}
			return method.invoke(input, args);
		}
	}
}
