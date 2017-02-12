package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class OutputPriorsHandler<V0, V> extends Namedly implements InvocationHandler, Output<V0> {
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
		String mn = method.getName();
		if ("name".equals(mn) && (null == args || args.length == 0)) return name;
		if (null != args && args.length == 1) switch (mn) {
		case "enqueue":
			return Stream.class.isAssignableFrom(args[0].getClass()) ? enqueue((Stream<V0>) args[0]) : enqueue((V0) args[0]);
		case "prior":
			return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy, (Converter<V0, V>) args[0]), Output.class);
		case "priors":
			return new OutputPriorsHandler<>((Output<V>) proxy, (Converter<List<V0>, List<V>>) args[0]).proxy(Output.class);
		}
		return method.invoke(output, args);
	}

	@Override
	public boolean enqueue(V0 item) {
		return output.enqueue(conv.apply(Arrays.asList(item)).get(0));
	}

	@Override
	public long enqueue(Stream<V0> items) {
		return output.enqueue(Streams.of(conv.apply(IO.list((Stream<V0>) items))));
	}
}