package net.butfly.albacore.io;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

class QueueImpl<IN, OUT> extends AbstractQueue<IN, OUT, Object> implements Queue<IN, OUT, Object> {
	private static final long serialVersionUID = 9187055541366585674L;
	private static final Logger logger = Logger.getLogger(QueueImpl.class);
	private final LinkedBlockingQueue<Object> impl;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QueueImpl(String name, long capacity, Map<Class, Converter<?, Long>> sizing) {
		super(name, capacity, t -> t, t -> (OUT) t);
		impl = new LinkedBlockingQueue<>((int) capacity);
		if (null != sizing) for (Class c : sizing.keySet())
			stats(c, sizing.get(c));
	}

	public QueueImpl(String name, long capacity) {
		this(name, capacity, null);
	}

	@Override
	protected boolean enqueueRaw(Object e) {
		if (null == e) return false;
		try {
			impl.put(e);
			return null != stats(Act.INPUT, e, () -> size());
		} catch (InterruptedException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	protected Object dequeueRaw() {
		try {
			return stats(Act.OUTPUT, impl.take(), () -> size());
		} catch (InterruptedException e) {
			logger.error("Dequeue failure", e);
			return null;
		}
	}

	@Override
	public long size() {
		return impl.size();
	}
}
