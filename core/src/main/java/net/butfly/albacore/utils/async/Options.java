package net.butfly.albacore.utils.async;

public class Options {
	enum ForkMode {
		NONE, PRODUCER, CONSUMER, LISTEN, @Deprecated BOTH_AND_WAIT
	}

	private ForkMode mode = ForkMode.NONE;
	private long timeout = -1;
	private long waiting = -1;

	/**
	 * Forking producer (Callable task) or consumer (Callback routine), another
	 * will continue in current thread.
	 * 
	 * @param producer
	 *            True to fork producer thread, False to fork consumer thread.
	 * @return
	 */
	public Options fork(boolean producer) {
		this.mode = producer ? ForkMode.PRODUCER : ForkMode.CONSUMER;
		return this;
	}

	/**
	 * Forking both producer and consumer to a new thread, and current thread is
	 * continuing immediately.
	 */
	public Options fork() {
		this.mode = ForkMode.LISTEN;
		return this;
	}

	/**
	 * Forking both producer and consumer to a new thread, and current thread is
	 * waiting for consumer finished with stepping {@link #waiting} ms.
	 * 
	 * @param waiting
	 *            Stepping of waiting and checking status of forked threads.
	 */
	@Deprecated
	public Options fork(long waiting) {
		this.mode = ForkMode.BOTH_AND_WAIT;
		this.waiting = waiting;
		return this;
	}

	ForkMode mode() {
		return mode;
	}

	long waiting() {
		return waiting;
	}

	public Options timeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	long timeout() {
		return timeout;
	}

//	public static final Options DEFAULT = new Options() {
//		public Options fork(boolean producer) {
//			throw new UnsupportedOperationException();
//		}
//
//		public Options fork() {
//			throw new UnsupportedOperationException();
//		}
//
//		@Deprecated
//		public Options fork(long waiting) {
//			throw new UnsupportedOperationException();
//		}
//
//		public Options timeout(long timeout) {
//			throw new UnsupportedOperationException();
//		}
//	};
}
