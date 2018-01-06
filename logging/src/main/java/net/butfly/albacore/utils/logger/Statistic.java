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
	private AtomicLong packsStep;
	private Supplier<String> detailing;
	private Function<E, Long> sizing;
	private Function<E, Long> stepping;

	private final long begin;
	private final AtomicLong statsed;

	private final AtomicLong packsInStep;
	private final AtomicLong bytesInStep;
	private final AtomicLong packsInTotal;
	private final AtomicLong bytesInTotal;

	protected Statistic(String logger) {
		lock = new ReentrantLock();
		this.logger = Logger.getLogger(logger);
		packsStep = new AtomicLong(DEFAULT_STEP - 1);
		packsInStep = new AtomicLong(0);
		bytesInStep = new AtomicLong(0);
		packsInTotal = new AtomicLong(0);
		bytesInTotal = new AtomicLong(0);
		begin = System.currentTimeMillis();
		statsed = new AtomicLong(begin);
		this.sizing = Syss::sizeOf;
		this.detailing = DEFAULT_EMPTY_DETAILING;
		this.stepping = e -> 1L;
	}

	public Statistic(Class<?> cls) {
		this(cls.getName() + ".stats");
	}

	public Statistic(Object obj) {
		this(obj.getClass().getName() + (obj instanceof Named ? ".Stats" : "." + ((Named) obj).name()));
	}

	public final Statistic<E> step(long step) {
		this.packsStep = new AtomicLong(step - 1);
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
			if (null == v) return;
			long size;
			if (sizing == null) size = 0;
			else try {
				Long s = sizing.apply(v);
				size = null == s ? 0 : s.longValue();
			} catch (Throwable t) {
				size = 0;
			}
			stats(stepping.apply(v), size);
		});
		return v;
	}

	public <C extends Iterable<E>> C stats(C i) {
		Logger.logexec(() -> {
			if (null == i) return;
			Iterator<E> it = i.iterator();
			if (!it.hasNext()) return;
			E e;
			while (it.hasNext())
				if (null != (e = it.next())) stats(e);
		});
		return i;
	}

	public <C extends Collection<E>> C stats(C c) {
		Logger.logexec(() -> {
			if (null == c || c.isEmpty()) return;
			for (E e : c)
				if (null != e) stats(e);
		});
		return c;
	}

	private void stats(long steps, long bytes) {
		packsInTotal.addAndGet(steps);
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (packsInStep.addAndGet(steps) > packsStep.get() && logger.isInfoEnabled() && lock.tryLock()) try {
			trace();
		} finally {
			lock.unlock();
		}
	}

	private void trace() {
		long now = System.currentTimeMillis();
		Result step, total;
		step = new Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - statsed.getAndSet(now));
		total = new Result(packsInTotal.get(), bytesInTotal.get(), System.currentTimeMillis() - begin);
		logger.debug(() -> traceDetail(step, total));
	}

	private String traceDetail(Result step, Result total) {
		String ss = null;
		if (null == detailing) ss = detailing.get();
		ss = null == ss ? "" : ", [" + ss + "]";
		String stepAvg = step.millis > 0 ? Long.toString(step.packs * 1000 / step.millis) : "no_time";
		String totalAvg = total.millis > 0 ? Long.toString(total.packs * 1000 / total.millis) : "no_time";
		return MessageFormat.format("Statistic: [Step: {0}/objs,{1},{2},{7} objs/s], [Total: {3}/objs,{4},{5},{8} objs/s] {6}.", //
				step.packs, formatKilo(step.bytes, "B"), formatMillis(step.millis), //
				total.packs, formatKilo(total.bytes, "B"), formatMillis(total.millis), //
				ss, stepAvg, totalAvg);
	}
}
