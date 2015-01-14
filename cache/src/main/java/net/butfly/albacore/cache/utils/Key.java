package net.butfly.albacore.cache.utils;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.ByteUtils;
import net.butfly.albacore.utils.encrypt.Algorithm.DigesterAlgorithm;
import net.butfly.albacore.utils.encrypt.DigesterEncryptor;

public class Key {
	private static final DigesterEncryptor encrypt = new DigesterEncryptor(DigesterAlgorithm.MD5);
	private Object obj;

	public <T extends Serializable> Key(T obj) {
		this.obj = obj;
	}

	public Key(Method method, Object... params) {
		try {
			String key = method.getDeclaringClass().getName() + method.getName();
			for (Object ob : params) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(ob);
				key = ByteUtils.byte2hex(bos.toByteArray()) + key;
			}
			key = encrypt.encrypt(key);
			this.obj = key;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public Object getObj() {
		return obj;
	}
}
