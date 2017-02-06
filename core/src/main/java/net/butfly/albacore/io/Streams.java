package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.log4j.Logger;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.async.Concurrents;

public final class Streams extends Utils {
	private static final Logger logger = Logger.getLogger(Streams.class);

	static <V> List<V> batch(Supplier<V> get, long batchSize) {
		List<V> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			try {
				V e = get.get();
				if (null != e) batch.add(e);
			} catch (Exception ex) {
				logger.warn("Dequeue fail but continue", ex);
			}
			if (batch.size() == 0) Concurrents.waitSleep();
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}
}
