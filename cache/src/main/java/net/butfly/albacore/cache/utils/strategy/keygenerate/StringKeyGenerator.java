package net.butfly.albacore.cache.utils.strategy.keygenerate;

import net.butfly.albacore.cache.utils.Key;
import net.butfly.albacore.exception.SystemException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringKeyGenerator implements IKeyGenerator {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	public static StringKeyGenerator instance = new StringKeyGenerator();

	public String getKey(Key o) {
		String key = null;
		if (o.getObj() instanceof String) {
			key = o.getObj().toString().replaceAll(" ", "");
		} else {
			throw new SystemException("SYS_119", "StringKeyGenerator can't support for this key ");
		}
		logger.debug("  value key：[" + key + "] successed generatored by StringKeyGenerator!!! ");
		return key;
	}
}
