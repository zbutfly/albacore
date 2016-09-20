package net.butfly.albacore.serder;

import java.io.Serializable;

public interface Serder<PRESENT, DATA> extends Serializable {
	<T extends PRESENT> DATA ser(T from);

	<T extends PRESENT> T der(DATA from, Class<T> to);

	default <RESULT> Serder<PRESENT, RESULT> next(Serder<DATA, RESULT> next, Class<DATA> data) {
		return new WrapperSerder<PRESENT, DATA, RESULT>(this, next, data);
	}

	default <RESULT> ArrableSerder<PRESENT, RESULT> next(ArrableSerder<DATA, RESULT> next, Class<DATA> data) {
		return new WrapperArrableSerder<PRESENT, DATA, RESULT>(this, next, data);
	}
}