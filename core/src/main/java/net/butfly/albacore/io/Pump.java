package net.butfly.albacore.io;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.logger.Logger;

public interface Pump {
	final static Logger logger = Logger.getLogger(Pump.class);
	final static UncaughtExceptionHandler DEFAULT_HANDLER = (t, e) -> {
		logger.error("Pump failure in one line [" + t.getName() + "]", e);
	};

	Pump interval(Runnable interval);

	Pump submit(Supplier<Boolean> stopping, int parallelism, String... nameSuffix);

	Pump start();

	Pump waiting();

	Pump waiting(long timeout, TimeUnit unit);

	Pump terminate();

	Pump terminate(long timeout, TimeUnit unit);
}
