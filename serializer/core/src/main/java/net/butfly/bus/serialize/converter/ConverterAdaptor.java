package net.butfly.bus.serialize.converter;

import net.butfly.albacore.utils.Instances;

public abstract class ConverterAdaptor<CC> {
	protected <C extends Converter<?, ?>> C getConverter(final Class<C> clazz) {
		return Instances.fetch(clazz);
	}

	public abstract <SRC, DST> CC create(Class<? extends Converter<SRC, DST>> converterClass);
}
