package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Output<V> extends Openable, Sizable {
	@Override
	default long size() {
		return 0;
	}

	/**
	 * basic, none blocking writing.
	 * 
	 * @param item
	 * @return
	 */
	boolean enqueue(V item, boolean block);

	@Deprecated
	default boolean enqueue(V item) {
		return enqueue(item, true);
	}

	default long enqueue(List<V> items) {
		long c = 0;
		while (full())
			Concurrents.waitSleep();
		for (V e : items)
			if (null != e) {
				if (enqueue(e)) c++;
				else logger().warn("Q enqueue failure though not full before, item maybe lost");
			}
		return c;
	}

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		return priors(Collections.convAs(conv));
	}

	default <V0> Output<V0> priors(Converter<List<V0>, List<V>> conv) {
		return InvocationHandler.proxy(new OutputPriorHandler<>(this, conv), Output.class);
	}

	final class OutputPriorHandler<V0, V> extends Namedly implements InvocationHandler {
		private final Output<V> output;
		private final Converter<List<V0>, List<V>> conv;

		public OutputPriorHandler(Output<V> output, Converter<List<V0>, List<V>> conv) {
			super(output.name() + "Prior");
			this.output = output;
			this.conv = conv;
		}

		@SuppressWarnings({ "unchecked" })
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			switch (method.getName()) {
			case "name":
				if (null == args || args.length == 0) return name;
				break;
			case "enqueue":
				switch (args.length) {
				case 2:
					return output.enqueue(conv.apply(Arrays.asList((V0) args[0])).get(0), (Boolean) args[1]);
				case 1:
					if (List.class.isAssignableFrom(args[0].getClass())) return output.enqueue(conv.apply((List<V0>) args[0]));
					else return output.enqueue(conv.apply(Arrays.asList((V0) args[0])).get(0));
				}
				break;
			case "prior":
				if (args.length == 1) return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy, //
						Collections.convAs((Converter<V0, V>) args[0])), Output.class);
				break;
			case "priors":
				if (args.length == 1) return InvocationHandler.proxy(new OutputPriorHandler<>((Output<V>) proxy,
						(Converter<List<V0>, List<V>>) args[0]), Output.class);
				break;
			}
			return method.invoke(output, args);
		}
	}

	@Deprecated
	default <V0> Output<V0> priorsWrap(Converter<List<V0>, List<V>> conv) {
		return new Output<V0>() {
			@Override
			public String name() {
				return Output.super.name() + "Prior";
			}

			@Override
			public boolean enqueue(V0 item, boolean block) {
				return Output.this.enqueue(conv.apply(Arrays.asList(item)).get(0), block);
			}

			@Override
			public boolean enqueue(V0 item) {
				return Output.this.enqueue(conv.apply(Arrays.asList(item)).get(0));
			}

			@Override
			public long enqueue(List<V0> items) {
				return Output.this.enqueue(conv.apply(items));
			}

			@Override
			public long size() {
				return Output.this.size();
			}

			@Override
			public boolean full() {
				return Output.this.full();
			}

			@Override
			public String toString() {
				return name();
			}

			@Override
			public void open(Runnable run) {
				Output.super.open(null);
				Output.this.open(run);
			}

			@Override
			public void close(Runnable run) {
				Output.super.close(null);
				Output.this.close(run);
			}

			@Override
			public Status status() {
				return Output.this.status();
			}

			@Override
			public long capacity() {
				return Output.this.capacity();
			}
		};
	}
}
