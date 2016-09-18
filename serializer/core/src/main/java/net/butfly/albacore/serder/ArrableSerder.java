package net.butfly.albacore.serder;

public interface ArrableSerder<D> extends Serder<D> {
	Object[] deserialize(D dst, Class<?>[] types);
}
