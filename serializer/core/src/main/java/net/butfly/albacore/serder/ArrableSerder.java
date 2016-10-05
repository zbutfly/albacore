package net.butfly.albacore.serder;

public interface ArrableSerder<PRESENT, DATA> extends Serder<PRESENT, DATA> {
	Object[] der(DATA from, Class<?>... tos);
}
