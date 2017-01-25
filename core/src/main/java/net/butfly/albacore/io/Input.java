package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.io.pump.BasicPump;
import net.butfly.albacore.io.pump.FanoutPump;
import net.butfly.albacore.io.pump.Pump;
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

	default <O2> Input<O2> thens(Converter<List<V>, List<O2>> conv) {
		return new Input<O2>() {
			@Override
			public String name() {
				return Input.super.name() + "Then";
			}

			@Override
			public O2 dequeue(boolean block) {
				return conv.apply(Arrays.asList(Input.this.dequeue(block))).get(0);
			}

			@Override
			public O2 dequeue() {
				return conv.apply(Arrays.asList(Input.this.dequeue())).get(0);
			}

			@Override
			public List<O2> dequeue(long batchSize) {
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
				Input.super.open();
				Input.this.open(run);
			}

			@Override
			public void close(Runnable run) {
				Input.super.close();
				Input.this.close(run);
			}

			@Override
			public Status status() {
				return Input.this.status();
			}
		};
	}

	default Pump pump(int parallelism, Output<V> dest) {
		return new BasicPump(this, parallelism, dest);
	}

	@SuppressWarnings("unchecked")
	default Pump pump(int parallelism, Output<V>... dests) {
		return new FanoutPump(this, parallelism, Arrays.asList(dests));
	}

	default Pump pump(int parallelism, List<? extends Output<V>> dests) {
		return new FanoutPump(this, parallelism, dests);
	}

}
