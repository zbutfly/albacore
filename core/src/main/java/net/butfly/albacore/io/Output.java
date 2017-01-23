package net.butfly.albacore.io;

import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Output<I> extends Openable {
	default long size() {
		return 0;
	}

	default long capacity() {
		return Long.MAX_VALUE;
	};

	default boolean full() {
		return size() >= capacity();
	}

	/**
	 * basic, none blocking writing.
	 * 
	 * @param d
	 * @return
	 */
	boolean enqueue0(I d);

	default long enqueue(List<I> items) {
		long c = 0;
		while (full())
			Concurrents.waitSleep();
		for (I e : items)
			if (null != e) {
				if (enqueue0(e)) c++;
				else logger().warn("Q enqueue failure though not full before, item maybe lost");
			}
		return c;
	}

	default <I0> Output<I0> prior(Converter<I0, I> conv) {
		return priors(Collections.convAs(conv));
	}

	default <I0> Output<I0> priors(Converter<List<I0>, List<I>> conv) {
		return new Output<I0>() {
			public String name() {
				return Output.super.name() + "Prior";
			}

			@Override
			public boolean enqueue0(I0 item) {
				List<I> dd = conv.apply(Arrays.asList(item));
				if (null == dd || dd.isEmpty()) return false;
				return Output.this.enqueue0(dd.get(0));
			}

			@Override
			public long enqueue(List<I0> items) {
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
			public void close() {
				Output.super.close();
				Output.this.close();
			}

			@Override
			public long capacity() {
				return Output.this.capacity();
			}
		};
	}
}
