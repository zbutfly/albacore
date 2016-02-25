package net.butfly.albacore.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BizUnitBase extends UnitBase implements BizUnit {
	private static final long serialVersionUID = 1L;
	// protected final Logger logger = Instances.fetch(new
	// Task.Callable<Logger>() {
	// @Override
	// public Logger create() {
	// return LoggerFactory.getLogger(BizUnitBase.this.getClass());
	// }
	// }, getClass());
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
}
