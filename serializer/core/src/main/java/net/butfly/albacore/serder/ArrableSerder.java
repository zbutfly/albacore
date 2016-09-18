package net.butfly.albacore.serder;

public interface ArrableSerder<S, D> extends Serder<S, D> {
	Object[] deserialize(D from, Class<?>[] tos);
}
