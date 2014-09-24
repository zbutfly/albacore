package net.butfly.albacore.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class AsyncUtils extends UtilsBase {
	private static ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool());
	private static ListeningExecutorService listener = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool());

	public static <IN, OUT> void invoke(final AsyncTask<OUT> task) {
		invoke(task, 0);
	}

	public static <IN, OUT> void invoke(final AsyncTask<OUT> task, final long timeout) {
		final ListenableFuture<OUT> future = executor.submit(task);
		future.addListener(new Runnable() {
			@Override
			public void run() {
				try {
					OUT r;
					if (timeout > 0) r = future.get(timeout, TimeUnit.MILLISECONDS);
					else r = future.get();
					task.getCallback().callback(r);
				} catch (Exception e) {}
			}
		}, listener);
	}

}
