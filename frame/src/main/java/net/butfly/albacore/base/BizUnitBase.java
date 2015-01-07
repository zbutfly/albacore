package net.butfly.albacore.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BizUnitBase extends UnitBase implements BizUnit {
	private static final long serialVersionUID = 1L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
}
