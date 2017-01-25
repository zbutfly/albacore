package net.butfly.albacore.io;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Output<V> extends Openable, Sizable {
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
	};

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

	class OutputPriorHandler<V0, V> extends Namedly implements InvocationHandler {
		private final Output<V> output;
		private final Converter<List<V0>, List<V>> conv;
		private Method nameMethod, enqueueMethod0, enqueueMethod1, enqueueMethod;

		private OutputPriorHandler(Output<V> output, Converter<List<V0>, List<V>> conv) {
			super(output.name() + "Then");
			this.output = output;
			this.conv = conv;
			try {
				this.nameMethod = output.getClass().getMethod("name");
			} catch (NoSuchMethodException | SecurityException e) {
				this.nameMethod = null;
			}
			try {
				this.enqueueMethod0 = output.getClass().getMethod("enqueue", List.class);
			} catch (NoSuchMethodException | SecurityException e) {
				this.enqueueMethod0 = null;
			}
			try {
				this.enqueueMethod1 = output.getClass().getMethod("enqueue", Object.class, boolean.class);
			} catch (NoSuchMethodException | SecurityException e) {
				this.enqueueMethod1 = null;
			}
			try {
				this.enqueueMethod = output.getClass().getMethod("enqueue", Object.class);
			} catch (NoSuchMethodException | SecurityException e) {
				this.enqueueMethod = null;
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if (nameMethod.equals(method)) return name;
			else if (enqueueMethod.equals(method)) return output.enqueue(conv.apply((List<V0>) args[0]));
			else if (enqueueMethod0.equals(method)) return output.enqueue(conv.apply(Arrays.asList((V0) args[0])).get(0),
					(boolean) args[1]);
			else if (enqueueMethod1.equals(method)) return output.enqueue(conv.apply(Arrays.asList((V0) args[0])).get(0));
			else return method.invoke(output, args);
		}
	}

	@SuppressWarnings("unchecked")
	default <V0> Output<V0> priors(Converter<List<V0>, List<V>> conv) {
		return (Output<V0>) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] { Output.class },
				new OutputPriorHandler<V0, V>(this, conv));
	}

	@Deprecated
	default <V0> Output<V0> priorsWrap(Converter<List<V0>, List<V>> conv) {
		return new Output<V0>() {
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
