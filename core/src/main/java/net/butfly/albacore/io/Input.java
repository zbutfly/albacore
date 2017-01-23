package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.io.pump.BasicPump;
import net.butfly.albacore.io.pump.FanoutPump;
import net.butfly.albacore.io.pump.Pump;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface Input<O> extends Openable {
	default long size() {
		return Long.MAX_VALUE;
	}

	default boolean empty() {
		return size() <= 0;
	}

	/**
	 * basic, none blocking reading.
	 * 
	 * @return null on empty
	 */
	O dequeue0();

	default List<O> dequeue(long batchSize) {
		List<O> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			O e = dequeue0();
			if (null != e) batch.add(e);
			if (batch.size() == 0) Concurrents.waitSleep();
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		this.close();
		return batch;
	}

	default <O2> Input<O2> then(Converter<O, O2> conv) {
		return thens(Collections.convAs(conv));
	}

	default <O2> Input<O2> thens(Converter<List<O>, List<O2>> conv) {
		return new Input<O2>() {
			@Override
			public String name() {
				return Input.super.name() + "Then";
			}

			@Override
			public O2 dequeue0() {
				O v = ((Input<O>) Input.this).dequeue0();
				if (null == v) return null;
				List<O2> l = conv.apply(Arrays.asList(v));
				if (null == l || l.isEmpty()) return null;
				return l.get(0);
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				List<O> l = Input.this.dequeue(batchSize);
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
			public void close() {
				Input.super.close();
				Input.this.close();
			}

			@Override
			public String toString() {
				return name();
			}
		};
	}

	default Pump pump(int parallelism, Output<O> dest) {
		return new BasicPump(this, parallelism, dest);
	}

	@SuppressWarnings("unchecked")
	default Pump pump(int parallelism, Output<O>... dests) {
		return new FanoutPump(this, parallelism, Arrays.asList(dests));
	}

	default Pump pump(int parallelism, List<? extends Output<O>> dests) {
		return new FanoutPump(this, parallelism, dests);
	}

}
