package net.butfly.albacore.io;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.TimeUnit;

@Deprecated
public class PooledPump<V> extends PumpBase<V> implements Pump {
	protected final Queue<V, V> pool;

	PooledPump(AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, int parallelism) {
		this(source, destination, parallelism, (t, e) -> {
			logger.error("Pump failure in one line [" + t.getName() + "]", e);
		});
	}

	PooledPump(AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, int parallelism, UncaughtExceptionHandler handler) {
		super(source, destination, parallelism, handler);
		this.pool = new SimpleJavaQueue<V>(null, parallelism);
	}

	@Override
	public Pump start() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pump waiting() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pump waiting(long timeout, TimeUnit unit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pump stop() {
		// TODO Auto-generated method stub
		return null;
	}
}
