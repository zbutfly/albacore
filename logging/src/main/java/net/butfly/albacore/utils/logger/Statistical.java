package net.butfly.albacore.utils.logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface Statistical {
	class Stats {
		private static final Map<Object, Statistic> IO_STATS = new ConcurrentHashMap<>();
	}

	default Statistic s() {
		return Stats.IO_STATS.get(this);
	}

	default void statistic(Statistic s) {
		if (null == s) Stats.IO_STATS.remove(this);
		else Stats.IO_STATS.put(this, s);
	}
}
