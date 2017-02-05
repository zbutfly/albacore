package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class OutputPriorsHandler<V0, V> extends Namedly implements InvocationHandler {
	private final Output<V> output;
	private final Converter<List<V0>, List<V>> conv;

	public OutputPriorsHandler(Output<V> output, Converter<List<V0>, List<V>> conv) {
		super(output.name() + "Prior");
		this.output = output;
		this.conv = conv;
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
				boolean stream = Stream.class.isAssignableFrom(args[0].getClass());
				List<V> l = conv.apply(stream ? ((Stream<V0>) args[0]).collect(Collectors.toList()) : Arrays.asList((V0) args[0]));
				return stream ? output.enqueue(l.parallelStream()) : output.enqueue(l.get(0));
			}
			break;
		case "prior":
			if (args.length == 1) return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy, (Converter<V0, V>) args[0]),
					Output.class);
			break;
		case "priors":
			if (args.length == 1) return new OutputPriorsHandler<>((Output<V>) proxy, (Converter<List<V0>, List<V>>) args[0]).proxy(
					Output.class);
			break;
		}
		return method.invoke(output, args);
	}
}