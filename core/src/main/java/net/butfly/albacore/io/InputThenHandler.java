package net.butfly.albacore.io;

import java.lang.reflect.Method;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class InputThenHandler<V, V1> extends Namedly implements InvocationHandler {
	private final Input<V> input;
	private final Converter<V, V1> conv;

	public InputThenHandler(Input<V> input, Converter<V, V1> conv) {
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
			if (args.length == 1 && Number.class.isAssignableFrom(args[0].getClass())) //
				return input.dequeue(((Number) args[0]).longValue()).map(conv);
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
}