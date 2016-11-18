package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

public interface Pump<V> extends Statistical<Pump<V>, V> {
	final static Logger logger = Logger.getLogger(Pump.class);

	Pump<V> batch(long batchSize);

	@SuppressWarnings("restriction")
	static void run(Pump<?>... pumps) {
		// handle kill -15, CTRL-C, kill -9
		Systems.handleSignal(sig -> {
			logger.error("Signal [" + sig.getName() + "][" + sig.getNumber() + "] caught, stopping all pumps");
			for (int i = 0; i < pumps.length; i++)
				((PumpBase<?>) pumps[i]).terminate();
		}, "TERM", "INT"/* , "KILL" */); // kill -9 catched by system/os
		for (int i = pumps.length - 1; i >= 0; i--)
			((PumpBase<?>) pumps[i]).start();
		for (int i = 0; i < pumps.length; i++)
			((PumpBase<?>) pumps[i]).waitForFinish();
	}
}
