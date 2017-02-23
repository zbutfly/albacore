package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class InputThensHandler<V, V1> extends Namedly implements InvocationHandler, Input<V1> {
	private final Input<V> input;
	private final Converter<Iterable<V>, Iterable<V1>> conv;
	private final int parallelism;

	public InputThensHandler(Input<V> input, Converter<Iterable<V>, Iterable<V1>> conv, int parallelism) {
		super(input.name() + "Then");
		this.input = input;
		this.conv = conv;
		this.parallelism = parallelism;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String mn = method.getName();
		if ("name".equals(mn) && (null == args || args.length == 0)) return name;
		switch (method.getName()) {
		case "dequeue":
			if (args.length == 1 && Number.class.isAssignableFrom(args[0].getClass())) return dequeue(((Number) args[0]).longValue());
			break;
		case "then":
			if (args.length == 1) return new InputThenHandler<>((Input<V>) proxy, (Converter<V, V1>) args[0]).proxy(Input.class);
			break;
		case "thens":
			if (args.length == 2 && Number.class.isAssignableFrom(args[1].getClass())) return new InputThensHandler<>((Input<V>) proxy,
					(Converter<Iterable<V>, Iterable<V1>>) args[0], ((Number) args[1]).intValue()).proxy(Input.class);
			break;
		}
		return method.invoke(input, args);
	}

	@Override
	public Stream<V1> dequeue(long batchSize) {
		return Streams.batchMap(parallelism, input.dequeue(batchSize), this.conv);
	}
}