package net.butfly.albacore.serializer;

import java.io.Serializable;
import java.lang.reflect.Field;

public interface ConfirmSerializer<S, D> extends Serializable {
	D serialize(S src);

	S deserialize(D dst, Class<S> srcClass);

	default String mapFieldName(Field field) {
		return field.getName();
	}

}
