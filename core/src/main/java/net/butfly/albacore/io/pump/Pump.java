package net.butfly.albacore.io.pump;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.butfly.albacore.io.FanOutput;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.Systems;

public interface Pump<V> extends Statistical<Pump<V>>, Openable {
	Pump<V> batch(long batchSize);

	@Override
	default void open(Runnable run) {
		Openable.super.open(() -> {
			// handle kill -15, CTRL-C, kill -9
			Systems.handleSignal(sig -> {
				close();
				System.err.println("Maybe you need to kill me manually: kill -9 " + Systems.pid() + ", \n" + Systems.threadsRunning()
						.count() + " threads remain: ");
				System.err.println("\t" + Systems.threadsRunning().map(t -> t.getId() + "[" + t.getName() + "]").collect(Collectors.joining(
						", ")));
			}, "TERM", "INT");
			/* , "KILL" */
			// kill -9 catched by system/os
			if (null != run) run.run();
		});
	}

	public static <V> Pump<V> pump(Input<V> input, int parallelism, Output<V> dest) {
		return new BasicPump<V>(input, parallelism, dest);
	}

	@SafeVarargs
	public static <V> Pump<V> pump(Input<V> input, int parallelism, Output<V>... dests) {
		return new BasicPump<>(input, parallelism, new FanOutput<V>(Arrays.asList(dests)));
	}

	public static <V> Pump<V> pump(Input<V> input, int parallelism, List<? extends Output<V>> dests) {
		return new BasicPump<>(input, parallelism, new FanOutput<V>(dests));
	}

}
