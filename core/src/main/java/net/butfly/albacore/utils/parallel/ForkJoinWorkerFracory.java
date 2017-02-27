package net.butfly.albacore.utils.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;

public class ForkJoinWorkerFracory implements ForkJoinWorkerThreadFactory {
	@Override
	public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		return new ForkJoinWorker(pool);
	}

	private static class ForkJoinWorker extends ForkJoinWorkerThread {
		protected ForkJoinWorker(ForkJoinPool pool) {
			super(pool);
		}
	}
}
