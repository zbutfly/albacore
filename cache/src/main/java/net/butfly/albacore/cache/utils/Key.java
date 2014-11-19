package net.butfly.albacore.cache.utils;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

import net.butfly.albacore.cache.utils.tool.MD5Encrypt;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.ByteUtils;

public class Key {
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
			key = MD5Encrypt.getEncrypt().encode(key.getBytes());
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
