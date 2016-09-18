package net.butfly.albacore.serder;

import java.lang.reflect.Field;

public interface Serder<D> extends ValueSerder<Object, D> {
	D serialize(Object src);

	@SuppressWarnings("rawtypes")
	Object deserialize(D dst, Class srcClass);

	default String mapFieldName(Field field) {
		return field.getName();
	}
}
