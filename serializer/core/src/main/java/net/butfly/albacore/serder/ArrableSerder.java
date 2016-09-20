package net.butfly.albacore.serder;

public interface ArrableSerder<S, D> extends Serder<S, D> {
	Object[] der(D from, Class<?>... tos);
}
