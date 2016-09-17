package net.butfly.bus.serialize.converter;

public interface Converter<SRC, DST> {
	DST serialize(SRC original);

	<S extends SRC> S deserialize(DST replaced);

	Class<SRC> getOriginalClass();

	// Class<DST> getReplacedClass();
}
