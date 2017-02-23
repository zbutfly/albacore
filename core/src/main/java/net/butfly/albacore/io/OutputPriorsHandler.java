package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;

public final class OutputPriorsHandler<V0, V> extends Namedly implements InvocationHandler, Output<V0> {
	private final Output<V> output;
	private final Converter<List<V0>, List<V>> conv;
	private final int batchSize;

	public OutputPriorsHandler(Output<V> output, Converter<List<V0>, List<V>> conv, int batchSize) {
		super(output.name() + "Prior");
		this.output = output;
		this.conv = conv;
		this.batchSize = batchSize;
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
					(Converter<List<V0>, List<V>>) args[0], ((Number) args[1]).intValue()).proxy(Output.class);
		}
		return method.invoke(output, args);
	}

	@Override
	public long enqueue(Stream<V0> items) {
		Iterator<V0> it = items.iterator();
		List<ListenableFuture<Long>> fs = new ArrayList<>();
		while (it.hasNext())
			fs.add(IO.listen(() -> output.enqueue(Streams.of(conv.apply(IO.list(Streams.batch(batchSize, it)))))));
		return IO.sum(fs, logger());
	}
}