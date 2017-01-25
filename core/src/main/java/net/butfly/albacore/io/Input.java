package net.butfly.albacore.io;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Input<V> extends Openable, Sizable {
	default long size() {
		return Long.MAX_VALUE;
	}

	/**
	 * basic, none blocking reading.
	 * 
	 * @return null on empty
	 */
	V dequeue(boolean block);

	@Deprecated
	default V dequeue() {
		return dequeue(true);
	}

	default List<V> dequeue(long batchSize) {
		List<V> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			V e = dequeue(false);
			if (null != e) batch.add(e);
			if (batch.size() == 0) Concurrents.waitSleep();
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		this.close();
		return batch;
	}

	default <V1> Input<V1> then(Converter<V, V1> conv) {
		return thens(Collections.convAs(conv));
	}

	class InputThenHandler<V, V1> extends Namedly implements InvocationHandler {
		private final Input<V> input;
		private final Converter<List<V>, List<V1>> conv;
		private Method nameMethod, dequeueMethod0, dequeueMethod1, dequeueMethod;

		private InputThenHandler(Input<V> input, Converter<List<V>, List<V1>> conv) {
			super(input.name() + "Then");
			this.input = input;
			this.conv = conv;
			try {
				this.nameMethod = input.getClass().getMethod("name");
			} catch (NoSuchMethodException | SecurityException e) {
				this.nameMethod = null;
			}
			try {
				this.dequeueMethod0 = input.getClass().getMethod("dequeue", boolean.class);
			} catch (NoSuchMethodException | SecurityException e) {
				this.dequeueMethod0 = null;
			}
			try {
				this.dequeueMethod1 = input.getClass().getMethod("dequeue");
			} catch (NoSuchMethodException | SecurityException e) {
				this.dequeueMethod1 = null;
			}
			try {
				this.dequeueMethod = input.getClass().getMethod("dequeue", long.class);
			} catch (NoSuchMethodException | SecurityException e) {
				this.dequeueMethod = null;
			}
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if (nameMethod.equals(method)) return name;
			else if (dequeueMethod.equals(method)) return conv.apply(input.dequeue((long) args[0]));
			else if (dequeueMethod0.equals(method)) return conv.apply(Arrays.asList(input.dequeue((boolean) args[0]))).get(0);
			else if (dequeueMethod1.equals(method)) return conv.apply(Arrays.asList(input.dequeue())).get(0);
			else return method.invoke(input, args);
		}
	}

	@SuppressWarnings("unchecked")
	default <V1> Input<V1> thens(Converter<List<V>, List<V1>> conv) {
		return (Input<V1>) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] { Input.class },
				new InputThenHandler<V, V1>(this, conv));
	}

	@Deprecated
	default <V1> Input<V1> thensWrap(Converter<List<V>, List<V1>> conv) {
		return new Input<V1>() {
			@Override
			public String name() {
				return Input.super.name() + "Then";
			}

			@Override
			public V1 dequeue(boolean block) {
				return conv.apply(Arrays.asList(Input.this.dequeue(block))).get(0);
			}

			@Override
			public V1 dequeue() {
				return conv.apply(Arrays.asList(Input.this.dequeue())).get(0);
			}

			@Override
			public List<V1> dequeue(long batchSize) {
				List<V> l = Input.this.dequeue(batchSize);
				return conv.apply(l);
			}

			@Override
			public long size() {
				return Input.this.size();
			}

			@Override
			public boolean empty() {
				return Input.this.empty();
			}

			@Override
			public void open(Runnable run) {
				Input.super.open(null);
				Input.this.open(run);
			}

			@Override
			public void close(Runnable run) {
				Input.super.close(null);
				Input.this.close(run);
			}

			@Override
			public Status status() {
				return Input.this.status();
			}
		};
	}
}
