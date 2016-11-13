package net.butfly.albacore.io;

import java.util.concurrent.TimeUnit;

import net.butfly.albacore.utils.logger.Logger;

public interface Pump {
	final static Logger logger = Logger.getLogger(Pump.class);

	Pump start();

	Pump waiting();

	Pump waiting(long timeout, TimeUnit unit);

	Pump stop();
}
