package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;
import net.butfly.albacore.utils.Exceptions;

public final class InputThensHandler<V, V1> extends Namedly implements InvocationHandler, Input<V1> {
	private final Input<V> input;
	private final Converter<List<V>, List<V1>> conv;
	private final int batchSize;

	public InputThensHandler(Input<V> input, Converter<List<V>, List<V1>> conv, int batchSize) {
		super(input.name() + "Then");
		this.input = input;
		this.conv = conv;
		this.batchSize = batchSize;
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
					(Converter<List<V>, List<V1>>) args[0], ((Number) args[1]).intValue()).proxy(Input.class);
			break;
		}
		return method.invoke(input, args);
	}

	@Override
	public Stream<V1> dequeue(long _batchSize) {
		Iterator<V> it = input.dequeue(_batchSize).iterator();
		Builder<ListenableFuture<List<V1>>> b = Stream.builder();
		while (it.hasNext())
			b.add(IO.listen(() -> conv.apply(IO.list(Streams.batch(batchSize, it)))));
		return b.build().flatMap(f -> {
			try {
				return Streams.of(f.get());
			} catch (InterruptedException e) {
				return Stream.empty();
			} catch (ExecutionException e) {
				logger().error("Dequeue fail", Exceptions.unwrap(e));
				return Stream.empty();
			}
		});
	}
}