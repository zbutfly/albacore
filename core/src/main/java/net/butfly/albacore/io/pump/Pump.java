package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

public interface Pump extends Statistical<Pump>, Openable {
	final static Logger logger = Logger.getLogger(Pump.class);

	Pump batch(long batchSize);

	void terminate();

	@Override
	default void opening() {
		// handle kill -15, CTRL-C, kill -9
		Systems.handleSignal(sig -> terminate(), "TERM", "INT");
		/* , "KILL" */
		// kill -9 catched by system/os
	}

	@Deprecated
	static void run(Pump... pumps) {
		for (int i = pumps.length - 1; i >= 0; i--)
			((PumpImpl) pumps[i]).open();
		for (int i = 0; i < pumps.length; i++)
			((PumpImpl) pumps[i]).close();
	}

}
