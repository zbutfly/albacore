package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.Systems;

public interface Pump extends Statistical<Pump>, Openable {
	Pump batch(long batchSize);

	@Override
	default void open() {
		Openable.super.open(() -> {
			// handle kill -15, CTRL-C, kill -9
			Systems.handleSignal(sig -> close(), "TERM", "INT");
			/* , "KILL" */
			// kill -9 catched by system/os
		});
	}

	@Deprecated
	static void run(Pump... pumps) {
		for (int i = pumps.length - 1; i >= 0; i--)
			((PumpImpl) pumps[i]).open();
		for (int i = 0; i < pumps.length; i++)
			((PumpImpl) pumps[i]).close();
	}

}
