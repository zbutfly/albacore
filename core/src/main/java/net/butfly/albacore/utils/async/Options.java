package net.butfly.albacore.utils.async;

import java.io.Serializable;

import net.butfly.albacore.utils.KeyUtils;

public final class Options implements Serializable {
	private static final long serialVersionUID = -7043260354737005676L;

	enum ForkMode {
		NONE, WHOLE, LATTER, EACH;
	}

	public Options() {}

	ForkMode mode = ForkMode.NONE;
	long timeout = -1;
	boolean unblock = false;

	/**
	 * Forking producer (Callable call) or consumer (Callback routine), another will continue in current
	 * thread.
	 * 
	 * @param both
	 *            </br> True to fork two thread, consumer thread will listen producer thread for
	 *            result,</br> False to fork consumer thread only, it will listen main thread for result
	 *            producing.
	 * @return
	 */
	public Options fork(boolean both) {
		return this.mode(both ? ForkMode.EACH : ForkMode.LATTER);
	}

	/**
	 * Forking both producer and consumer to a new thread.
	 */
	public Options fork() {
		return this.mode(ForkMode.WHOLE);
	}

	public Options timeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public Options block() {
		this.unblock = false;
		return this;
	}

	public Options unblock() {
		this.unblock = true;
		return this;
	}

	private Options mode(ForkMode mode) {
		this.mode = mode;
		this.unblock = this.mode != ForkMode.NONE;
		return this;
	}

	/* continuous options */

	private static final int RETRIES_MAX = 100;
	// default no continuous, < 0 for infinity
	int repeat = 1;
	// default no retry, finish on any failure, < 0 for forever retry (RETRIES_MAX times limited)
	int retry = 0;
	// default start in same thread
	int concurrence = 0;
	// default no waiting interval
	long interval = 0;

	public Options continuous(int repeat) {
		this.repeat = repeat;
		return this;
	}

	public Options continuous() {
		this.repeat = -1;
		return this;
	}

	public Options discontinuous() {
		this.repeat = 0;
		return this;
	}

	public Options retries(int retries) {
		this.retry = retries < 0 || retries > RETRIES_MAX ? RETRIES_MAX : retries;
		return this;
	}

	public Options concurrence(int concurrence) {
		this.concurrence = concurrence;
		return this;
	}

	public Options interval(long milliseconds) {
		this.interval = milliseconds;
		return this;
	}

	public String toString() {
		String[] fields = new String[7];
		fields[0] = Integer.toString(this.mode.ordinal());
		fields[1] = Long.toString(this.timeout);
		fields[2] = Boolean.toString(this.unblock);
		fields[3] = Integer.toString(this.repeat);
		fields[4] = Integer.toString(this.retry);
		fields[5] = Integer.toString(this.concurrence);
		fields[6] = Long.toString(this.interval);
		return KeyUtils.join(':', fields);
	}

	public Options(String format) {
		String[] fields = format.split(":");
		this.mode = ForkMode.values()[Integer.parseInt(fields[0])];
		this.timeout = Long.parseLong(fields[1]);
		this.unblock = Boolean.parseBoolean(fields[2]);
		this.repeat = Integer.parseInt(fields[3]);
		this.retry = Integer.parseInt(fields[4]);
		this.concurrence = Integer.parseInt(fields[5]);
		this.interval = Long.parseLong(fields[6]);
	}
}
