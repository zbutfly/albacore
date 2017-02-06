package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class InputThensHandler<V, V1> extends Namedly implements InvocationHandler {
	private final Input<V> input;
	private final Converter<List<V>, List<V1>> conv;

	public InputThensHandler(Input<V> input, Converter<List<V>, List<V1>> conv) {
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
			if (args.length == 1 && Number.class.isAssignableFrom(args[0].getClass())) {
				return conv.apply(input.dequeue(((Number) args[0]).longValue()).collect(Collectors.toList())).parallelStream();
			} else if (args.length == 0) return conv.apply(Arrays.asList(input.dequeue())).get(0);
			break;
		case "then":
			if (args.length == 1) return new InputThenHandler<>((Input<V>) proxy, (Converter<V, V1>) args[0]).proxy(Input.class);
			break;
		case "thens":
			if (args.length == 1) return new InputThensHandler<>((Input<V>) proxy, (Converter<List<V>, List<V1>>) args[0]).proxy(
					Input.class);
			break;
		}
		return method.invoke(input, args);
	}
}