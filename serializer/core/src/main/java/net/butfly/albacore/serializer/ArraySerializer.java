package net.butfly.albacore.serializer;

public interface ArraySerializer<D> extends Serializer<D> {
	Object[] deserialize(D dst, Class<?>[] types);
}
