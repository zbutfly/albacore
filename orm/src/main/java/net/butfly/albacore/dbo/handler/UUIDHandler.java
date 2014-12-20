package net.butfly.albacore.dbo.handler;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.UUID;

import org.apache.ibatis.type.JdbcType;

public class UUIDHandler extends JdbcTypeHandler<UUID, byte[]> {
	private static Constructor<UUID> constructor;
	private static Method data;
	static {
		try {
			constructor = UUID.class.getDeclaredConstructor(byte[].class);
			constructor.setAccessible(true);
			data = UUID.class.getDeclaredMethod("data");
			data.setAccessible(true);
		} catch (Exception e) {}
	}

	@Override
	protected byte[] serialize(UUID object) throws Exception {
		if (null == object) return null;
		return (byte[]) data.invoke(object);
	}

	@Override
	protected UUID deserialize(byte[] value) {
		if (null == value) return null;
		try {
			return constructor.newInstance(value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected JdbcType getSQLType() {
		return JdbcType.BINARY;
	}
}
