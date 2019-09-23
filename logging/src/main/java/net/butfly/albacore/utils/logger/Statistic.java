package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.utils.logger.LogExec.tryExec;
import static net.butfly.albacore.utils.logger.StatsUtils.formatKilo;
import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.utils.SizeOfSupport;

public class Statistic {
	protected static final Logger COMMON_LOG = Logger.getLogger(Statistic.class);

	private static final long DEFAULT_STEP = 1000;
	private static final Supplier<String> DEFAULT_EMPTY_DETAILING = () -> null;
	private final ReentrantLock lock;

	private String name;
	protected final Logger logger;
	private AtomicLong stepSize;
	private Supplier<String> detailing;
	private Function<Object, Long> sizing;
	private Function<Object, Long> batchSizing;

	private Function<Object, String> infoing;
	private long sampleStep;
	private final AtomicLong sampleCounter = new AtomicLong();
	@SuppressWarnings("rawtypes")
	private Consumer sampling = null;

	public final long begin;

	private final AtomicLong statsed;
	private final AtomicLong packStep;
	private final AtomicLong byteStep;
	private final AtomicLong packTotal;
	private final AtomicLong byteTotal;

	private final AtomicLong batchs;

	private final AtomicLong spentTotal;
	private final AtomicLong ignoreTotal;
	private final String loggerName;

	protected Statistic(String loggerName) {
		lock = new ReentrantLock();
		this.loggerName = loggerName;
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
		infoing = e -> null;
		sampleStep = -1;
		detailing = DEFAULT_EMPTY_DETAILING;
		batchSizing = e -> 1L;
		name = "[STATISTIC]";
		if (!enabled()) COMMON_LOG.warn("Statistic [" + loggerName + "] registered but the logging level DEBUG disabled!!");
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
		if (step <= 0) stepSize.set(Long.MAX_VALUE);
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
	public final <E> Statistic infoing(Function<E, String> infoing) {
		this.infoing = o -> {
			try {
				return infoing.apply((E) o);
			} catch (Exception e) {
				return null;
			}

		};
		return this;
	}

	public final <E> Statistic sampling(long step, Consumer<E> sampling) {
		this.sampleStep = step;
		this.sampling = sampling;
		return this;
	}

	public final Statistic detailing(Supplier<String> detailing) {
		this.detailing = detailing;
		return this;
	}

	// stating
	public <E> E stats(E v) {
		tryStats(() -> stats0(v));
		return v;
	}

	protected <E> void stats0(E v) {
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
		stats0(v, s, size);
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
			stats(v, System.currentTimeMillis() - now);
		}
	}

	public <E> void statsOutN(E v, Consumer<E> use) {
		long now = System.currentTimeMillis();
		try {
			use.accept(v);
		} finally {
			stats(v, System.currentTimeMillis() - now);
		}
	}

	public <E, R, C extends Collection<E>> R statsOut(C c, Function<C, R> use) {
		AtomicReference<R> r = new AtomicReference<>();
		statsOuts(c, cc -> r.set(use.apply(cc)));
		return r.get();
	}

	public <E, C extends Collection<E>> void statsOuts(C c, Consumer<C> use) {
		long now = System.currentTimeMillis();
		try {
			use.accept(c);
		} finally {
			stats(c, System.currentTimeMillis() - now);
		}
	}

	public <E, C extends Collection<E>> void stats(C c, long spent) {
		if (null != c && !c.isEmpty()) {
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <E> void stats(E v, long spent) {
		if (null == v) return;

		spentTotal.addAndGet(spent);
		if (v instanceof Collection) stats((Collection) v);
		if (v instanceof Iterable) stats((Iterable) v);
		else if (v.getClass().isArray()) stats((Object[]) v);
		else stats(v);
	}

	public Snapshot snapshot() {
		return new Snapshot(0);
	}

	protected Snapshot snapshot(long step) {
		return new Snapshot(step);
	}

	public void trace(long step, String sampling) {
		Snapshot s = snapshot(step);
		if (s.stepPacks > 0) logger.debug(s.info(sampling)::toString);
	}

	private String getSampleInfo(Object v) {
		return null != infoing && enabledMore() ? infoing.apply(v) : null;
	}

	public final AtomicReference<Object> last = new AtomicReference<>();

	@SuppressWarnings("unchecked")
	protected void stats0(Object v, long steps, long bytes) {
		last.lazySet(v);
		if (stepSize.get() < 0) return;
		packTotal.addAndGet(steps);
		byteTotal.addAndGet(bytes < 0 ? 0 : bytes);
		byteStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (null != sampling && accu(sampleCounter, steps, sampleStep) == 0) //
			sampling.accept(v);
		long step = accu(packStep, steps, stepSize.get());
		if (step > 0 && logger.isInfoEnabled() && lock.tryLock()) try {
			// if (packStep.get() > stepSize.get())
			trace(step, getSampleInfo(v));
		} finally {
			lock.unlock();
		}
	}

	private long accu(AtomicLong atomic, long step, long limit) {
		AtomicLong r = new AtomicLong();
		atomic.accumulateAndGet(step, (o, s) -> {
			long ss = o + s;
			if (ss <= limit) return ss;
			r.set(ss);
			return 0;
		});
		return r.get();
	}

	public class Snapshot implements Serializable {
		private static final long serialVersionUID = -7093004807672632693L;
		public String table = null;
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
		private String sample = null;

		private Snapshot(long step) {
			// synchronized (Statistic.this) {
			long now = System.currentTimeMillis();
			this.stepPacks = step > 0 ? step : packStep.get();
			this.stepBytes = step > 0 ? byteStep.getAndSet(0) : byteStep.get();
			this.stepMillis = now - (step > 0 ? statsed.getAndSet(now) : statsed.get());

			this.totalPacks = packTotal.get();
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
			return totalPacks > 0 || stepPacks > 0;
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

		public Snapshot info(String sample) {
			this.sample = sample;
			return this;
		}

		@Override
		public String toString() {
			if (!valid()) return "no_stats";
			StringBuilder info = new StringBuilder(name).append(":");
			if (stepPacks > 0 && stepAvg > 0) {
				info.append("[Step:").append(stepPacks).append(" objs,");
				if (stepBytes > 0) info.append(formatKilo(stepBytes, "B")).append(",");
				if (stepMillis > 0) info.append(formatMillis(stepMillis)).append(",");
				info.append(stepAvg).append(" objs/s]");
			}
			info.append("[Total: ").append(totalPacks).append(" objs,");
			if (totalBytes > 0) info.append(formatKilo(totalBytes, "B")).append(",");
			info.append(formatMillis(totalMillis));
			if (totalAvg > 0) info.append(",").append(totalAvg).append(" objs/s]");
			if (null != detail) info.append("\n\t[").append(detail).append("]");
			if (null != extra) info.append("\n\t").append(extra);
			if (null != sample && sample.length() > 0) info.append("\n\tSample: ").append(sample);
			return info.toString();
		}
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

	public void flush() {
		trace(0, null);
		packStep.set(0);
		byteStep.set(0);
		packTotal.set(0);
		byteTotal.set(0);
		spentTotal.set(0);
		ignoreTotal.set(0);
		batchs.set(0);
		statsed.set(begin);
		COMMON_LOG.debug("Statistic [" + loggerName + "] flushed!!");
	}
}
