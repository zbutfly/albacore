package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class OutputPriorHandler<V0, V> extends Namedly implements InvocationHandler {
	private final Output<V> output;
	private final Converter<V0, V> conv;

	public OutputPriorHandler(Output<V> output, Converter<V0, V> conv) {
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
				if (Stream.class.isAssignableFrom(args[0].getClass())) return output.enqueue(((Stream<V0>) args[0]).map(conv));
				else return output.enqueue(conv.apply(((V0) args[0])));
			}
			break;
		case "prior":
			if (args.length == 1) return new OutputPriorHandler<>((Output<V>) proxy, (Converter<V0, V>) args[0]).proxy(Output.class);
			break;
		case "priors":
			if (args.length == 2) return new OutputPriorsHandler<>((Output<V>) proxy, (Converter<List<V0>, List<V>>) args[0],
					((Number) args[1]).intValue()).proxy(Output.class);
			break;
		}
		return method.invoke(output, args);
	}
}