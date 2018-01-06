package net.butfly.albacore.utils.logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface Statistical {
	class Stats {
		private static final Map<Object, Statistic<?>> IO_STATS = new ConcurrentHashMap<>();
	}

	default <E> E stats(E e) {
		Logger.logexec(() -> {
			@SuppressWarnings("unchecked")
			Statistic<E> s = (Statistic<E>) Stats.IO_STATS.get(this);
			if (null == s) return;
			s.stats(e);
		});
		return e;
	}

	default <I extends Iterable<E>, E> I stats(I i) {
		Logger.logexec(() -> {
			@SuppressWarnings("unchecked")
			Statistic<E> s = (Statistic<E>) Stats.IO_STATS.get(this);
			if (null == s) return;
			s.stats(i);
		});
		return i;
	}

	default <C extends Collection<E>, E> C stats(C i) {
		Logger.logexec(() -> {
			@SuppressWarnings("unchecked")
			Statistic<E> s = (Statistic<E>) Stats.IO_STATS.get(this);
			if (null == s) return;
			s.stats(i);
		});
		return i;
	}

	@SuppressWarnings("unchecked")
	default <E> Statistic<E> trace(Class<E> cls) {
		return (Statistic<E>) Stats.IO_STATS.computeIfAbsent(this, Statistic<E>::new);
	}
}
