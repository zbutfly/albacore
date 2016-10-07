package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Concurrents;
import scala.Tuple2;

public abstract class AbstractQueue<IN, OUT, DATA> implements Queue<IN, OUT, DATA>, Statistical {
	private static final long serialVersionUID = 7082069605335516902L;

	private final AtomicLong capacity;
	private final AtomicBoolean orderlyRead = new AtomicBoolean(false);
	private final AtomicBoolean orderlyWrite = new AtomicBoolean(false);

	protected final Tuple2<Converter<IN, DATA>, Converter<DATA, OUT>> conv;

	final String name;

	protected AbstractQueue(String name, long capacity, Converter<IN, DATA> in, Converter<DATA, OUT> out) {
		Reflections.noneNull("", in, out);
		this.name = name;
		this.capacity = new AtomicLong(capacity);
		conv = new Tuple2<>(in, out);
	}

	abstract protected boolean enqueueRaw(DATA e);

	abstract protected DATA dequeueRaw();

	@Override
	public final String name() {
		return name;
	}

	@Override
	public long capacity() {
		return capacity.get();
	}

	@Override
	public final boolean isReadOrderly() {
		return orderlyRead.get();
	}

	@Override
	public final boolean isWriteOrderly() {
		return orderlyWrite.get();
	}

	@Override
	public final void setReadOrderly(boolean orderly) {
		orderlyRead.set(orderly);
	}

	@Override
	public final void setWriteOrderly(boolean orderly) {
		orderlyWrite.set(orderly);
	}

	// Dont override

	@Override
	public final boolean empty() {
		return Queue.super.empty();
	}

	@Override
	public final boolean full() {
		return Queue.super.full();
	}

	@Override
	public boolean enqueue(IN e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return (enqueueRaw(conv._1.apply(e)));
	}

	@Override
	public final OUT dequeue() {
		while (true) {
			DATA e = dequeueRaw();
			if (null != e) return conv._2.apply(e);
			else if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	@Override
	public List<OUT> dequeue(long batchSize) {
		List<OUT> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			DATA e = dequeueRaw();
			if (null != e) {
				batch.add(conv._2.apply(e));
				if (empty()) gc();
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		if (empty()) gc();
		return batch;
	}
}
