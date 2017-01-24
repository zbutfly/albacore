package net.butfly.albacore.io;

import java.util.Arrays;
import java.util.List;

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

	default <I0> Output<I0> priors(Converter<List<I0>, List<V>> conv) {
		return new Output<I0>() {
			public String name() {
				return Output.super.name() + "Prior";
			}

			@Override
			public boolean enqueue(I0 item, boolean block) {
				return Output.this.enqueue(conv.apply(Arrays.asList(item)).get(0), block);
			}

			@Override
			public boolean enqueue(I0 item) {
				return Output.this.enqueue(conv.apply(Arrays.asList(item)).get(0));
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
