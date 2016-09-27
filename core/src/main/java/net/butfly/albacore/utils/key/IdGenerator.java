package net.butfly.albacore.utils.key;

import net.butfly.albacore.utils.logger.Logger;

public abstract class IdGenerator<K> {
	protected final Logger logger = Logger.getLogger(this.getClass());

	public abstract K generate();

	protected abstract long machine();
}