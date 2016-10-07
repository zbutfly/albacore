package net.butfly.albacore.io;

import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

class QueueImpl<IN, OUT> extends AbstractQueue<IN, OUT> implements Queue<IN, OUT> {
	private static final long serialVersionUID = 9187055541366585674L;
	private static final Logger logger = Logger.getLogger(QueueImpl.class);
	private final LinkedBlockingQueue<IN> impl;
	private final Converter<IN, OUT> conv;

	public QueueImpl(String name, long capacity, Converter<IN, OUT> conv) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
		this.conv = conv;
	}

	@Override
	protected boolean enqueueRaw(IN e) {
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
	protected OUT dequeueRaw() {
		try {
			return stats(Act.OUTPUT, conv.apply(impl.take()), () -> size());
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
