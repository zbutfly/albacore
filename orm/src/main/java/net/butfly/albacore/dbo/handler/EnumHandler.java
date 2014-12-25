package net.butfly.albacore.dbo.handler;

import net.butfly.albacore.utils.EnumUtils;

import org.apache.ibatis.type.JdbcType;

public class EnumHandler<E extends Enum<E>> extends JdbcTypeHandler<E, Byte> {
	private Class<E> type;

	public EnumHandler(Class<E> type) throws NoSuchMethodException, SecurityException {
		this.type = type;
	}

	@Override
	protected Byte serialize(E object) throws Exception {
		return EnumUtils.value(object);
	}

	@Override
	protected E deserialize(Byte value) throws Exception {
		if (value == null) return null;
		return EnumUtils.parse(type, value);
	}

	@Override
	protected JdbcType getSQLType() {
		return JdbcType.TINYINT;
	}
}
