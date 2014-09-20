package net.butfly.albacore.base;

import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

public abstract class BizUnitBase extends UnitBase implements BizUnit {
	private static final long serialVersionUID = 1L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
}
