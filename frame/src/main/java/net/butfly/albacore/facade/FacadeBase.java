package net.butfly.albacore.facade;

import net.butfly.albacore.base.BizUnitBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FacadeBase extends BizUnitBase implements Facade {
	private static final long serialVersionUID = -4087689783635914433L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
}
