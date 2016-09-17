package net.butfly.albacore.serializer;

import java.lang.reflect.Field;

public interface Serializer<D> extends ConfirmSerializer<Object, D> {
	D serialize(Object src);

	@SuppressWarnings("rawtypes")
	Object deserialize(D dst, Class srcClass);

	default String mapFieldName(Field field) {
		return field.getName();
	}
}
