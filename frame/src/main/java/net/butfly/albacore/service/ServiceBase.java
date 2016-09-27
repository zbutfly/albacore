package net.butfly.albacore.service;

import net.butfly.albacore.base.BizUnitBase;

import net.butfly.albacore.utils.logger.Logger;

public abstract class ServiceBase extends BizUnitBase implements Service {
	private static final long serialVersionUID = 1L;
	protected final Logger logger = Logger.getLogger(this.getClass());
}
