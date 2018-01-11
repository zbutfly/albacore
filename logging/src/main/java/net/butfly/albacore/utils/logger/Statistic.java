package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.utils.logger.LogExec.tryExec;
import static net.butfly.albacore.utils.logger.StatsUtils.formatKilo;
import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.utils.Syss;
import net.butfly.albacore.utils.logger.StatsUtils.Result;

public class Statistic {
	private static final long DEFAULT_STEP = 1000;
	private static final Supplier<String> DEFAULT_EMPTY_DETAILING = () -> null;
	private final ReentrantLock lock;

	protected final Logger logger;
	private AtomicLong stepSize;
	private Supplier<String> detailing;
	private Function<Object, Long> sizing;
	private Function<Object, Long> stepping;
	private String name;

	private final long begin;

	private final AtomicLong statsed;
	private final AtomicLong packStep;
	private final AtomicLong byteStep;
	private final AtomicLong packTotal;
	private final AtomicLong byteTotal;

	private final AtomicLong batchs;

	private final AtomicLong spentTotal;
	private final AtomicLong ignoreTotal;

	protected Statistic(String loggerName) {
		lock = new ReentrantLock();
		logger = Logger.getLogger(loggerName);
		stepSize = new AtomicLong(DEFAULT_STEP - 1);
		packStep = new AtomicLong(0);
		byteStep = new AtomicLong(0);
		packTotal = new AtomicLong(0);
		byteTotal = new AtomicLong(0);
		spentTotal = new AtomicLong(0);
		ignoreTotal = new AtomicLong(0);

		batchs = new AtomicLong(0);
		begin = System.currentTimeMillis();
		statsed = new AtomicLong(begin);
		sizing = Syss::sizeOf;
		detailing = DEFAULT_EMPTY_DETAILING;
		stepping = e -> 1L;
		name = "[STATISTIC]";
		logger.warn("Statistic [" + loggerName + "] registered, do you enable the logging level DEBUG?");
	}

	public Statistic(Class<?> ownerClass) {
		this(ownerClass.getName());
	}

	public Statistic(Object owner) {
		this(owner.getClass().getName());
		if (owner instanceof Named) name(((Named) owner).name());
	}

	public final Statistic name(String ownerName) {
		this.name = "[STATS: " + ownerName + "]";
		return this;
	}

	/**
	 * @param step
	 *            <li>0: count but print manually
	 *            <li>less than 0: do not change anything
	 */
	public final Statistic step(long step) {
		if (step == 0) stepSize.set(Long.MAX_VALUE);
		else if (step > 0) stepSize.set(step - 1);
		return this;
	}

	@SuppressWarnings("unchecked")
	public final <E> Statistic sizing(Function<E, Long> sizing) {
		this.sizing = o -> {
			try {
				return sizing.apply((E) o);
			} catch (ClassCastException e) {
				return 0L;
			}
		};
		return this;
	}

	@SuppressWarnings("unchecked")
	public final <E> Statistic stepping(Function<E, Long> stepping) {
		this.stepping = o -> {
			try {
				return stepping.apply((E) o);
			} catch (ClassCastException e) {
				return 1L;
			}
		};
		return this;
	}

	public final Statistic detailing(Supplier<String> detailing) {
		this.detailing = detailing;
		return this;
	}

	public <E> E stats(E v) {
		tryStats(() -> {
			if (stepSize.get() < 0) return;
			if (null == v) return;
			long size;
			if (sizing == null) size = 0;
			else try {
				Long s = sizing.apply(v);
				size = null == s ? 0 : s.longValue();
			} catch (Throwable t) {
				size = 0;
			}
			long s = stepping.apply(v);
			if (s > 1) batchs.incrementAndGet();
			stats(s, size);
		});
		return v;
	}

	public <E> E statsIn(Supplier<E> get) {
		long now = System.currentTimeMillis();
		E vv = null;
		try {
			return vv = get.get();
		} finally {
			long spent = System.currentTimeMillis() - now;
			E v = vv;
			tryStats(() -> {
				if (stepSize.get() < 0) return;
				if (null == v) return;
				spentTotal.addAndGet(spent);
				long size;
				if (sizing == null) size = 0;
				else try {
					Long s = sizing.apply(v);
					size = null == s ? 0 : s.longValue();
				} catch (Throwable t) {
					size = 0;
				}
				long s = stepping.apply(v);
				if (s > 1) batchs.incrementAndGet();
				stats(s, size);
			});
		}
	}

	public <E, R> R statsOut(E v, Function<E, R> use) {
		long now = System.currentTimeMillis();
		try {
			return use.apply(v);
		} finally {
			long spent = System.currentTimeMillis() - now;
			tryStats(() -> {
				if (stepSize.get() < 0) return;
				if (null == v) return;
				spentTotal.addAndGet(spent);
				long size;
				if (sizing == null) size = 0;
				else try {
					Long s = sizing.apply(v);
					size = null == s ? 0 : s.longValue();
				} catch (Throwable t) {
					size = 0;
				}
				long s = stepping.apply(v);
				if (s > 1) batchs.incrementAndGet();
				stats(s, size);
			});
		}
	}

	public <E, C extends Iterable<E>> C stats(C i) {
		tryStats(() -> {
			if (stepSize.get() < 0) return;
			if (null == i) return;
			int b = 0;
			for (E e : i)
				if (null != e) {
					stats(e);
					b++;
				}
			if (b > 0) batchs.incrementAndGet();
		});
		return i;
	}

	public <E, C extends Collection<E>> C stats(C c) {
		tryStats(() -> {
			if (stepSize.get() < 0) return;
			if (null == c || c.isEmpty()) return;
			int b = 0;
			for (E e : c)
				if (null != e) {
					stats(e);
					b++;
				}
			if (b > 0) batchs.incrementAndGet();
		});
		return c;
	}

	public <E, C extends Collection<E>> C statsIns(Supplier<C> get) {
		long now = System.currentTimeMillis();
		C vv = null;
		try {
			return vv = get.get();
		} finally {
			long spent = System.currentTimeMillis() - now;
			C c = vv;
			tryStats(() -> {
				if (stepSize.get() < 0) return;
				if (null == c || c.isEmpty()) return;
				spentTotal.addAndGet(spent);
				int b = 0;
				for (E e : c)
					if (null != e) {
						stats(e);
						b++;
					}
				if (b > 0) batchs.incrementAndGet();
			});
		}
	}

	public <E, R> R statsOuts(Collection<E> c, Function<Collection<E>, R> use) {
		long now = System.currentTimeMillis();
		try {
			return use.apply(c);
		} finally {
			long spent = System.currentTimeMillis() - now;
			tryStats(() -> {
				if (stepSize.get() < 0) return;
				if (null == c || c.isEmpty()) return;
				spentTotal.addAndGet(spent);
				int b = 0;
				for (E e : c)
					if (null != e) {
						stats(e);
						b++;
					}
				if (b > 0) batchs.incrementAndGet();
			});
		}
	}

	private void stats(long steps, long bytes) {
		if (stepSize.get() < 0) return;
		packTotal.addAndGet(steps);
		byteTotal.addAndGet(bytes < 0 ? 0 : bytes);
		byteStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (packStep.addAndGet(steps) > stepSize.get() && logger.isInfoEnabled() && lock.tryLock()) try {
			trace();
		} finally {
			lock.unlock();
		}
	}

	public void trace() {
		long now = System.currentTimeMillis();
		Result step, total;
		total = new Result(packTotal.get(), byteTotal.get(), System.currentTimeMillis() - begin);
		if (total.packs <= 0) return;
		step = new Result(packStep.getAndSet(0), byteStep.getAndSet(0), now - statsed.getAndSet(now));
		if (step.packs <= 0) return;
		logger.debug(() -> traceDetail(step, total));
	}

	private CharSequence traceDetail(Result step, Result total) {
		String stepAvg = step.millis > 0 ? Long.toString(step.packs * 1000 / step.millis) : "no_time";
		String totalAvg = total.millis > 0 ? Long.toString(total.packs * 1000 / total.millis) : "no_time";
		StringBuilder info = new StringBuilder(name)//
				.append(":[Step:").append(step.packs).append("/objs,").append(formatKilo(step.bytes, "B")).append(",").append(formatMillis(
						step.millis)).append(",").append(stepAvg).append(" objs/s], ")//
				.append("[Total: ").append(total.packs).append("/objs,").append(formatKilo(total.bytes, "B")).append(",").append(
						formatMillis(total.millis)).append(",").append(totalAvg).append(" objs/s]");
		appendDetail(info);
		long b;
		if ((b = batchs.get()) > 0) info.append(", [Average batch size: ").append(total.packs / b).append("]");
		if ((b = spentTotal.get()) > 0) info.append(", [Average 1000 obj spent: ").append(b * 1000 / total.packs).append(" ms]");
		if ((b = ignoreTotal.get()) > 0) info.append(", [Logger ignore: ").append(b).append("]");
		return info;
	}

	private void appendDetail(StringBuilder info) {
		if (null == detailing) return;
		String ss = detailing.get();
		if (null == ss) return;
		info.append("\n\t[").append(ss).append("]");
	}

	private void tryStats(Runnable r) {
		if (!tryExec(r)) ignoreTotal.incrementAndGet();
	}
}
