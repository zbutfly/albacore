package net.butfly.albacore.serder;

import com.google.common.reflect.TypeToken;

public interface ArrableSerder<PRESENT, DATA> extends Serder<PRESENT, DATA> {
	@SuppressWarnings("unchecked")
	PRESENT[] der(DATA from, TypeToken<? extends PRESENT>... tos);
}
