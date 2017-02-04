package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;
import net.butfly.albacore.utils.Collections;

public final class OutputPriorsHandler<V0, V> extends Namedly implements InvocationHandler {
	private final Output<V> output;
	private final Converter<List<V0>, List<V>> conv;
	private final int parallelism;

	public OutputPriorsHandler(Output<V> output, Converter<List<V0>, List<V>> conv, int parallelism) {
		super(output.name() + "Prior");
		this.output = output;
		this.conv = conv;
		this.parallelism = parallelism;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		switch (method.getName()) {
		case "name":
			if (null == args || args.length == 0) return name;
			break;
		case "enqueue":
			if (args.length == 1) {
				if (Stream.class.isAssignableFrom(args[0].getClass())) return output.enqueue(Collections.chopped((Stream<V0>) args[0],
						parallelism).map(conv).reduce(Collections.merging()).get().parallelStream());
				else return output.enqueue(conv.apply(Arrays.asList((V0) args[0])).get(0));
			}
			break;
		case "prior":
			if (args.length == 1) return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy, (Converter<V0, V>) args[0]),
					Output.class);
			break;
		case "priors":
			if (args.length == 2) return new OutputPriorsHandler<>((Output<V>) proxy, (Converter<List<V0>, List<V>>) args[0],
					((Number) args[1]).intValue()).proxy(Output.class);
			break;
		}
		return method.invoke(output, args);
	}
}