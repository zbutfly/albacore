package net.butfly.albacore.utils.async;

public class Options {
	enum ForkMode {
		NONE(false), PRODUCER(false), CONSUMER(true), LISTEN(true);
		boolean async;

		private ForkMode(boolean async) {
			this.async = async;
		}
	}

	ForkMode mode = ForkMode.NONE;
	long timeout = -1;
	boolean unblock = ForkMode.NONE.async;

	/**
	 * Forking producer (Callable callable) or consumer (Callback routine), another
	 * will continue in current thread.
	 * 
	 * @param producer
	 *            True to fork producer thread, False to fork consumer thread.
	 * @return
	 */
	public Options fork(boolean producer) {
		return this.mode(producer ? ForkMode.PRODUCER : ForkMode.CONSUMER);
	}

	/**
	 * Forking both producer and consumer to a new thread, and current thread is
	 * continuing immediately.
	 */
	public Options fork() {
		return this.mode(ForkMode.LISTEN);
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

	public boolean needCallback() {
		return this.mode.async;
	}

	private Options mode(ForkMode mode) {
		this.mode = mode;
		this.unblock = mode.async;
		return this;
	}
}
