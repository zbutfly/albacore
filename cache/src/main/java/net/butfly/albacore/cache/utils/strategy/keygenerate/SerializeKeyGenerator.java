package net.butfly.albacore.cache.utils.strategy.keygenerate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import net.butfly.albacore.cache.utils.Key;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.encrypt.Algorithm.DigesterAlgorithm;
import net.butfly.albacore.utils.encrypt.DigesterEncryptor;
import net.butfly.albacore.utils.logger.Logger;

public class SerializeKeyGenerator implements IKeyGenerator {
	private static final DigesterEncryptor encrypt = new DigesterEncryptor(DigesterAlgorithm.MD5);
	protected final Logger logger = Logger.getLogger(this.getClass());

	@Override
	public String getKey(Key o) {
		String key = null;
		try {
			if (o.getObj() instanceof String) {
				key = o.getObj().toString().replaceAll(" ", "");
			} else {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(o.getObj());
				key = encrypt.encrypt(key);
			}
			logger.debug("  value key：[" + key + "] successed generatored by SerializeKeyGenerator!!! ");
		} catch (IOException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
		return key;
	}
}
