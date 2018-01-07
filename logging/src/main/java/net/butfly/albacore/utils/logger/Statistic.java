package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.utils.logger.StatsUtils.formatKilo;
import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.utils.Syss;
import net.butfly.albacore.utils.logger.StatsUtils.Result;

public class Statistic<E> {
	private static final long DEFAULT_STEP = 1000;
	private static final Supplier<String> DEFAULT_EMPTY_DETAILING = () -> null;
	private final ReentrantLock lock;

	protected final Logger logger;
	private AtomicLong stepSize;
	private Supplier<String> detailing;
	private Function<E, Long> sizing;
	private Function<E, Long> stepping;
	private String name;

	private final long begin;

	private final AtomicLong statsed;
	private final AtomicLong packStep;
	private final AtomicLong byteStep;
	private final AtomicLong packTotal;
	private final AtomicLong byteTotal;

	private final AtomicLong batchs;

	protected Statistic(String logger) {
		lock = new ReentrantLock();
		this.logger = Logger.getLogger(logger);
		stepSize = new AtomicLong(DEFAULT_STEP - 1);
		packStep = new AtomicLong(0);
		byteStep = new AtomicLong(0);
		packTotal = new AtomicLong(0);
		byteTotal = new AtomicLong(0);
		batchs = new AtomicLong(0);
		begin = System.currentTimeMillis();
		statsed = new AtomicLong(begin);
		this.sizing = Syss::sizeOf;
		this.detailing = DEFAULT_EMPTY_DETAILING;
		this.stepping = e -> 1L;
		this.name = "[STATISTIC]";
		this.logger.warn("Statistic [" + logger + "] registered, do you enable the logging level DEBUG?");
	}

	public Statistic(Class<?> cls) {
		this(cls.getName() + ".stats");
	}

	public Statistic(Object obj) {
		this(obj.getClass().getName() + (obj instanceof Named ? ".Stats" : "." + ((Named) obj).name()));
	}

	public final Statistic<E> name(String name) {
		this.name = "[STATS: " + name + "]";
		return this;
	}

	/**
	 * @param step
	 *            <li>0: count but print manually
	 *            <li>less than 0: do not change anything
	 */
	public final Statistic<E> step(long step) {
		if (step == 0) stepSize.set(Long.MAX_VALUE);
		else if (step > 0) stepSize.set(step - 1);
		return this;
	}

	public final Statistic<E> sizing(Function<E, Long> sizing) {
		this.sizing = sizing;
		return this;
	}

	public final Statistic<E> stepping(Function<E, Long> stepping) {
		this.stepping = stepping;
		return this;
	}

	public final Statistic<E> detailing(Supplier<String> detailing) {
		this.detailing = detailing;
		return this;
	}

	public E stats(E v) {
		Logger.logexec(() -> {
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

	public <C extends Iterable<E>> C stats(C i) {
		Logger.logexec(() -> {
			if (stepSize.get() < 0) return;
			if (null == i) return;
			Iterator<E> it = i.iterator();
			if (!it.hasNext()) return;
			E e;
			while (it.hasNext())
				if (null != (e = it.next())) stats(e);
			batchs.incrementAndGet();
		});
		return i;
	}

	public <C extends Collection<E>> C stats(C c) {
		Logger.logexec(() -> {
			if (stepSize.get() < 0) return;
			if (null == c || c.isEmpty()) return;
			for (E e : c)
				if (null != e) stats(e);
			batchs.incrementAndGet();
		});
		return c;
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
		step = new Result(packStep.getAndSet(0), byteStep.getAndSet(0), now - statsed.getAndSet(now));
		total = new Result(packTotal.get(), byteTotal.get(), System.currentTimeMillis() - begin);
		logger.debug(() -> traceDetail(step, total));
	}

	private String traceDetail(Result step, Result total) {
		String stepAvg = step.millis > 0 ? Long.toString(step.packs * 1000 / step.millis) : "no_time";
		String totalAvg = total.millis > 0 ? Long.toString(total.packs * 1000 / total.millis) : "no_time";
		String info = MessageFormat.format(": [Step: {0}/objs,{1},{2},{6} objs/s], [Total: {3}/objs,{4},{5},{7} objs/s]", //
				step.packs, formatKilo(step.bytes, "B"), formatMillis(step.millis), //
				total.packs, formatKilo(total.bytes, "B"), formatMillis(total.millis), //
				stepAvg, totalAvg);
		info = name + appendDetail(info);
		long b;
		if ((b = batchs.get()) > 0) info += ", [Batch: " + total.packs / b + "]";
		return info;
	}

	private String appendDetail(String info) {
		if (null == detailing) return info;
		String ss = detailing.get();
		if (null == ss) return info;
		return info + ", [" + ss + "]";
	}
}
