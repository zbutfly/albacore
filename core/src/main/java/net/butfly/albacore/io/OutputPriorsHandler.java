package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class OutputPriorsHandler<V0, V> extends Namedly implements InvocationHandler, Output<V0> {
	private final Output<V> output;
	private final Converter<Iterable<V0>, Iterable<V>> conv;
	private final int parallelism;

	public OutputPriorsHandler(Output<V> output, Converter<Iterable<V0>, Iterable<V>> conv, int parallelism) {
		super(output.name() + "Prior");
		this.output = output;
		this.conv = conv;
		this.parallelism = parallelism;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String mn = method.getName();
		if ("name".equals(mn) && (null == args || args.length == 0)) return name;
		if (null != args && args.length == 1) switch (mn) {
		case "enqueue":
			if (Stream.class.isAssignableFrom(args[0].getClass())) return enqueue((Stream<V0>) args[0]);
		case "prior":
			return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy, (Converter<V0, V>) args[0]), Output.class);
		case "priors":
			if (args.length == 2 && Number.class.isAssignableFrom(args[1].getClass())) return new OutputPriorsHandler<>((Output<V>) proxy,
					(Converter<Iterable<V0>, Iterable<V>>) args[0], ((Number) args[1]).intValue()).proxy(Output.class);
		}
		return method.invoke(output, args);
	}

	@Override
	public long enqueue(Stream<V0> items) {
		return IO.run(() -> Streams.batch(parallelism, items).mapToLong(s0 -> output.enqueue(Streams.of(conv.apply((Iterable<V0>) () -> s0
				.iterator())))).sum());
	}
}