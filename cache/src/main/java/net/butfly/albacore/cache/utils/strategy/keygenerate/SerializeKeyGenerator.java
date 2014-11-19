package net.butfly.albacore.cache.utils.strategy.keygenerate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import net.butfly.albacore.cache.utils.Key;
import net.butfly.albacore.cache.utils.tool.MD5Encrypt;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

public class SerializeKeyGenerator implements IKeyGenerator {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public String getKey(Key o) {
		String key = null;
		try {
			if (o.getObj() instanceof String) {
				key = o.getObj().toString().replaceAll(" ", "");
			} else {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(o.getObj());
				key = MD5Encrypt.getEncrypt().encode(bos.toByteArray());
			}
			logger.debug("  value key：[" + key + "] successed generatored by SerializeKeyGenerator!!! ");
		} catch (IOException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
		return key;
	}
}
