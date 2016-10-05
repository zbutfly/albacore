package net.butfly.albacore.serder;

import java.io.Serializable;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.lambda.Converter;

public interface Serder<PRESENT, DATA> extends Serializable {
	<T extends PRESENT> DATA ser(T from);

	<T extends PRESENT> T der(DATA from, TypeToken<T> to);

	default <RESULT> Serder<PRESENT, RESULT> then(Serder<DATA, RESULT> next, TypeToken<DATA> data) {
		return new WrapperSerder<PRESENT, DATA, RESULT>(this, next, data);
	}

	default <RESULT> ArrableSerder<PRESENT, RESULT> then(ArrableSerder<DATA, RESULT> next, TypeToken<DATA> data) {
		return new WrapperArrableSerder<PRESENT, DATA, RESULT>(this, next, data);
	}

	default Converter<PRESENT, DATA> converter() {
		return this::ser;
	}

	@SuppressWarnings("serial")
	default Converter<DATA, PRESENT> unconverter() {
		return v -> null == v ? null : der(v, new TypeToken<PRESENT>() {});
	}
}