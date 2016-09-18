package net.butfly.albacore.serder;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Comparator;

import com.google.common.collect.Ordering;

import net.butfly.albacore.utils.CaseFormat;

public interface Serder<PRESENT, DATA> extends Serializable {
	static final CaseFormat DEFAULT_SRC_FORMAT = CaseFormat.LOWER_CAMEL;
	static final CaseFormat DEFAULT_DST_FORMAT = CaseFormat.UPPER_UNDERSCORE;

	DATA serialize(Object from);

	Object deserialize(DATA from, Class<?> to);

	default <T> DATA serializeT(T from) {
		return serialize(from);
	}

	@SuppressWarnings("unchecked")
	default <T> T deserializeT(DATA from, Class<T> to) {
		return (T) deserialize(from, to);
	}

	String parseQualifier(Field f);
}