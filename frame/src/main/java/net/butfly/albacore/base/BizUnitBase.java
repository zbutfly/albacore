package net.butfly.albacore.base;

import net.butfly.albacore.utils.logger.Logger;

public abstract class BizUnitBase extends UnitBase implements BizUnit {
	private static final long serialVersionUID = 1L;
//	protected final Logger logger = Instances.fetch(new Runnable.Callable<Logger>() {
//		@Override
//		public Logger create() {
//			return Logger.getLogger(BizUnitBase.this.getClass());
//		}
//	}, getClass());
	protected final Logger logger = Logger.getLogger(this.getClass());
}
