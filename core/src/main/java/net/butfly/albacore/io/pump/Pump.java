package net.butfly.albacore.io.pump;

import java.util.concurrent.TimeUnit;

import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.logger.Logger;

public interface Pump<V> extends Statistical<Pump<V>, V> {
	final static Logger logger = Logger.getLogger(Pump.class);

	Pump<V> start();

	Pump<V> waiting();

	Pump<V> waiting(long timeout, TimeUnit unit);

	Pump<V> stop();

	Pump<V> batch(long batchSize);
}
