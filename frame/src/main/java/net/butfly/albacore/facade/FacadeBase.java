package net.butfly.albacore.facade;

import net.butfly.albacore.base.BizUnitBase;
import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

public abstract class FacadeBase extends BizUnitBase implements Facade {
	private static final long serialVersionUID = -4087689783635914433L;
	private boolean debug = true;

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
}
