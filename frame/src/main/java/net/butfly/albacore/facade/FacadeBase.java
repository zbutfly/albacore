package net.butfly.albacore.facade;

import net.butfly.albacore.base.BizUnitBase;
import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

public abstract class FacadeBase extends BizUnitBase implements Facade {
	private static final long serialVersionUID = -4087689783635914433L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
}
