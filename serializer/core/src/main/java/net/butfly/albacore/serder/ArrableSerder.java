package net.butfly.albacore.serder;

public interface ArrableSerder<PRESENT, DATA> extends Serder<PRESENT, DATA> {
	@SuppressWarnings("unchecked")
	PRESENT[] der(DATA from, Class<? extends PRESENT>... tos);
}
