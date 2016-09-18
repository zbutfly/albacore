package net.butfly.albacore.serder;

import java.io.Serializable;
import java.lang.reflect.Field;

public interface ValueSerder<S, D> extends Serializable {
	D serialize(S src);

	S deserialize(D dst, Class<S> srcClass);

	default String mapFieldName(Field field) {
		return field.getName();
	}

}
