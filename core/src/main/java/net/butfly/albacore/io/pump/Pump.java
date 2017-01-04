package net.butfly.albacore.io.pump;

import java.util.concurrent.atomic.AtomicBoolean;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

public interface Pump extends Statistical<Pump>, Openable {
	final static Logger logger = Logger.getLogger(Pump.class);
	static final AtomicBoolean SIGNAL_HANDLED = new AtomicBoolean(false);

	Pump batch(long batchSize);

	static void run(Pump... pumps) {
		// handle kill -15, CTRL-C, kill -9
		if (!SIGNAL_HANDLED.getAndSet(true)) Systems.handleSignal(sig -> {
			for (int i = 0; i < pumps.length; i++)
				((PumpImpl) pumps[i]).terminate();
		}, "TERM", "INT"/* , "KILL" */); // kill -9 catched by system/os
		for (int i = pumps.length - 1; i >= 0; i--)
			((PumpImpl) pumps[i]).open();
		for (int i = 0; i < pumps.length; i++)
			((PumpImpl) pumps[i]).close();
	}
}
