package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.utils.logger.LogExec.tryExec;
import static net.butfly.albacore.utils.logger.StatsUtils.formatKilo;
import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.utils.SizeOfSupport;
import net.butfly.albacore.utils.logger.StatsUtils.Result;

public class Statistic {
	private static final long DEFAULT_STEP = 1000;
	private static final Supplier<String> DEFAULT_EMPTY_DETAILING = () -> null;
	private final ReentrantLock lock;

	protected final Logger logger;
	private AtomicLong stepSize;
	private Supplier<String> detailing;
	private Function<Object, Long> sizing;
	private Function<Object, Long> batchSizing;
	private Function<Object, String> sampling;
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
		sizing = o -> o instanceof SizeOfSupport ? ((SizeOfSupport) o)._sizeOf() : SizeOfSupport.sizeOf(o);
		sampling = e -> null;
		detailing = DEFAULT_EMPTY_DETAILING;
		batchSizing = e -> 1L;
		name = "[STATISTIC]";
		if (!enabled()) logger.warn("Statistic [" + loggerName + "] registered but the logging level DEBUG disabled!!");
	}

	public Statistic(Class<?> ownerClass) {
		this(ownerClass.getName());
	}

	public Statistic(Object owner) {
		this(owner.getClass().getName());
		if (owner instanceof Named) name(((Named) owner).name());
	}

	// configurating the stats
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
	public final <E> Statistic batchSizeCalcing(Function<E, Long> batchSizing) {
		this.batchSizing = o -> {
			try {
				return batchSizing.apply((E) o);
			} catch (Exception e) {
				return 1L;
			}
		};
		return this;
	}

	@SuppressWarnings("unchecked")
	public final <E> Statistic sampling(Function<E, String> sampling) {
		this.sampling = o -> {
			try {
				return sampling.apply((E) o);
			} catch (Exception e) {
				return null;
			}

		};
		return this;
	}

	public final Statistic detailing(Supplier<String> detailing) {
		this.detailing = detailing;
		return this;
	}

	// stating
	public <E> E stats(E v) {
		tryStats(() -> {
			if (stepSize.get() < 0 || null == v) return;
			long size;
			if (sizing == null || !enabledMore()) size = 0;
			else try {
				Long s = sizing.apply(v);
				size = null == s ? 0 : s.longValue();
			} catch (Throwable t) {
				size = 0;
			}
			long s = batchSizing.apply(v);
			if (s > 1) batchs.incrementAndGet();
			stats(v, s, size);
		});
		return v;
	}

	@SafeVarargs
	public final <E> E[] stats(E... v) {
		tryStats(() -> {
			if (stepSize.get() < 0 || null == v || v.length == 0) return;
			int b = 0;
			for (E e : v) if (null != e) {
				stats(e);
				b++;
			}
			if (b > 1) batchs.incrementAndGet();
		});
		return v;
	}

	public <E, C extends Iterable<E>> C stats(C i) {
		tryStats(() -> {
			if (stepSize.get() < 0 || null == i) return;
			int b = 0;
			for (E e : i) if (null != e) {
				stats(e);
				b++;
			}
			if (b > 1) batchs.incrementAndGet();
		});
		return i;
	}

	private static boolean empty(Collection<?> l) {
		return null == l || l.isEmpty();
	}

	public <E, C extends Collection<E>> C stats(C c) {
		tryStats(() -> {
			if (stepSize.get() < 0 || empty(c)) return;
			int b = 0;
			for (E e : c) if (null != e) {
				stats(e);
				b++;
			}
			if (b > 1) batchs.incrementAndGet();
		});
		return c;
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
				spentTotal.addAndGet(spent);
				if (null != v) stats(v);
			});
		}
	}

	public <E> E[] statsInA(Supplier<E[]> get) {
		long now = System.currentTimeMillis();
		E[] vv = null;
		try {
			return vv = get.get();
		} finally {
			long spent = System.currentTimeMillis() - now;
			E[] c = vv;
			tryStats(() -> {
				spentTotal.addAndGet(spent);
				if (null != c && c.length > 0) stats(c);
			});
		}
	}

	public <E, C extends Collection<E>> C statsIns(Supplier<C> get) {
		long now = System.currentTimeMillis();
		C vv = null;
		try {
			return vv = get.get();
		} finally {
			if (empty(vv)) return vv;
			long spent = System.currentTimeMillis() - now;
			C c = vv;
			tryStats(() -> {
				spentTotal.addAndGet(spent);
				if (null != c && !c.isEmpty()) stats(c);
			});
		}
	}

	public <E, R> R statsOut(E v, Function<E, R> use) {
		long now = System.currentTimeMillis();
		try {
			return use.apply(v);
		} finally {
			timing(v, now);
		}
	}

	@Deprecated
	public <E> void statsOut(E v, Consumer<E> use) {
		statsOutN(v, use);
	}

	public <E> void statsOutN(E v, Consumer<E> use) {
		long now = System.currentTimeMillis();
		try {
			use.accept(v);
		} finally {
			timing(v, now);
		}
	}

	private <E> void timing(E v, long start) {
		if (null == v) return;
		long spent = System.currentTimeMillis() - start;
		tryStats(() -> {
			spentTotal.addAndGet(spent);
			if (null != v) stats(v);
		});
	}

	public <E, R, C extends Collection<E>> R statsOuts(C c, Function<C, R> use) {
		long now = System.currentTimeMillis();
		try {
			return use.apply(c);
		} finally {
			if (null != c && !c.isEmpty()) {
				long spent = System.currentTimeMillis() - now;
				tryStats(() -> {
					if (stepSize.get() < 0 || empty(c)) return;
					spentTotal.addAndGet(spent);
					if (null != c && !c.isEmpty()) {
						int b = 0;
						for (E e : c) if (null != e) {
							stats(e);
							b++;
						}
						if (b > 1) batchs.incrementAndGet();
					}
				});
			}
		}
	}

	@Deprecated
	public <E, C extends Collection<E>> void statsOuts(C c, Consumer<C> use) {
		statsOutsN(c, use);
	}

	public <E, C extends Collection<E>> void statsOutsN(C c, Consumer<C> use) {
		long now = System.currentTimeMillis();
		try {
			use.accept(c);
		} finally {
			long spent = System.currentTimeMillis() - now;
			tryStats(() -> {
				if (stepSize.get() < 0 || empty(c)) return;
				spentTotal.addAndGet(spent);
				if (null != c && !c.isEmpty()) {
					int b = 0;
					for (E e : c) if (null != e) {
						stats(e);
						b++;
					}
					if (b > 1) batchs.incrementAndGet();
				}
			});
		}
	}

	public void trace(String sampling) {
		long now = System.currentTimeMillis();
		Result step, total;
		total = new Result(packTotal.get(), byteTotal.get(), now - begin);
		if (total.packs <= 0) return;
		step = new Result(packStep.getAndSet(0), byteStep.getAndSet(0), now - statsed.getAndSet(now));
		if (step.packs <= 0) return;
		logger.debug(() -> traceDetail(step, total, sampling));
	}

	private void stats(Object v, long steps, long bytes) {
		if (stepSize.get() < 0) return;
		packTotal.addAndGet(steps);
		byteTotal.addAndGet(bytes < 0 ? 0 : bytes);
		byteStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (packStep.addAndGet(steps) > stepSize.get() && logger.isInfoEnabled() && lock.tryLock()) try {
			trace(enabledMore() ? sampling.apply(v) : null);
		} finally {
			lock.unlock();
		}
	}

	public class Snapshot implements Serializable {
		private static final long serialVersionUID = -7093004807672632693L;
		public final long stepPacks;
		public final long stepBytes;
		public final long stepMillis;
		public final long stepAvg; // calc

		public final long totalPacks;
		public final long totalBytes;
		public final long totalMillis;
		public final long totalAvg; // calc

		public final long batchsCount;
		public final long spentTotal;
		public final long ignoreTotal;

		public final String detail;
		public final CharSequence extra;

		private Snapshot(long totalPacks, long stepPacks) {
			// synchronized (Statistic.this) {
			long now = System.currentTimeMillis();
			this.stepPacks = stepPacks;
			this.stepBytes = byteStep.getAndSet(0);
			this.stepMillis = statsed.getAndSet(now);

			this.totalPacks = totalPacks;
			this.totalBytes = byteTotal.get();
			this.totalMillis = now - begin;

			this.stepAvg = stepPacks > 0 && stepMillis > 0 ? stepPacks * 1000 / stepMillis : -1;
			this.totalAvg = totalPacks > 0 && totalMillis > 0 ? totalPacks * 1000 / totalMillis : -1;

			this.batchsCount = batchs.get();
			this.spentTotal = Statistic.this.spentTotal.get();
			this.ignoreTotal = Statistic.this.ignoreTotal.get();

			this.detail = null == detailing || !enabledMore() ? null : detailing.get();
			this.extra = appendExtra(totalPacks, totalBytes);
			// }
		}

		private boolean valid() {
			return totalPacks > 0 && stepPacks > 0;
		}

		private CharSequence appendExtra(long totalPacks, long totalBytes) {
			StringBuilder info = new StringBuilder();
			long b;
			if ((b = batchsCount) > 0) info.append("[Average batch size: ").append(totalPacks / b).append("]");
			if ((b = spentTotal) > 0) {
				if (info.length() > 0) info.append(", ");
				info.append("[Average 1000 obj spent: ").append(b * 1000 / totalPacks).append(" ms]");
			}
			if ((b = ignoreTotal) > 0) {
				if (info.length() > 0) info.append(", ");
				info.append("[Logger ignore: ").append(b).append("]");
			}
			return info.length() > 0 ? info : null;
		}

		@Override
		public String toString() {
			if (!valid()) return "no_stats";
			StringBuilder info = new StringBuilder(name)//
					.append(":[Step:").append(stepPacks).append("/objs,").append(formatKilo(stepBytes, "B")).append(",")//
					.append(formatMillis(stepMillis)).append(",").append(stepAvg > 0 ? stepAvg : "no_time").append(" objs/s], ")//
					.append("[Total: ").append(totalPacks).append("/objs,").append(formatKilo(totalBytes, "B")).append(",")//
					.append(formatMillis(totalMillis)).append(",").append(totalAvg > 0 ? stepAvg : "no_time").append(" objs/s]");
			if (null != detail) info.append("\n\t[").append(detail).append("]");
			if (null != extra) info.append("\n\t").append(extra);
			return detail;
		}
	}

	public Snapshot snapshot() {
		return new Snapshot(packTotal.get(), packStep.getAndSet(0));
	}

	private CharSequence traceDetail(Result step, Result total, String sampling) {
		if (null != sampling && sampling.length() > 0) return snapshot().toString() + "\n\tSample: " + sampling;
		else return snapshot().toString();
	}

	private void tryStats(Runnable r) {
		if (!tryExec(r)) ignoreTotal.incrementAndGet();
	}

	public boolean enabled() {
		return logger.isDebugEnabled();
	}

	public boolean enabledMore() {
		return logger.isTraceEnabled();
	}
}
