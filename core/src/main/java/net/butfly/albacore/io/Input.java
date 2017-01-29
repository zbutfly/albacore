package net.butfly.albacore.io;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.InvocationHandler;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Input<V> extends Openable, Sizable {
	@Override
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

	default <V1> Input<V1> thens(Converter<List<V>, List<V1>> conv) {
		return new InputThenHandler<>(this, conv).proxy(Input.class);
	}

	final class InputThenHandler<V, V1> extends Namedly implements InvocationHandler {
		private final Input<V> input;
		private final Converter<List<V>, List<V1>> conv;

		public InputThenHandler(Input<V> input, Converter<List<V>, List<V1>> conv) {
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
				if (null == args || args.length == 0) return conv.apply(Arrays.asList(input.dequeue())).get(0);
				if (args.length == 1) {
					if (Number.class.isAssignableFrom(args[0].getClass())) return conv.apply(input.dequeue(((Number) args[0]).longValue()));
					if (Boolean.class.isAssignableFrom(args[0].getClass())) return conv.apply(Arrays.asList(input.dequeue(
							((Boolean) args[0]).booleanValue()))).get(0);
				}
				break;
			case "then":
				if (args.length == 1) return InvocationHandler.proxy(new InputThenHandler<>((Input<V>) proxy, //
						Collections.convAs((Converter<V, V1>) args[0])), Input.class);
				break;
			case "thens":
				if (args.length == 1) return InvocationHandler.proxy(new InputThenHandler<>((Input<V>) proxy,
						(Converter<List<V>, List<V1>>) args[0]), Input.class);
				break;
			}
			return method.invoke(input, args);
		}
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
