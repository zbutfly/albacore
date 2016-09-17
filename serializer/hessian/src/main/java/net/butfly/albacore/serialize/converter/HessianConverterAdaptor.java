package net.butfly.albacore.serialize.converter;

import java.io.IOException;

import com.caucho.hessian.io.AbstractDeserializer;
import com.caucho.hessian.io.AbstractHessianInput;
import com.caucho.hessian.io.AbstractSerializer;
import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Deserializer;
import com.caucho.hessian.io.HessianProtocolException;
import com.caucho.hessian.io.Serializer;

import net.butfly.bus.serialize.converter.Converter;
import net.butfly.bus.serialize.converter.ConverterAdaptor;

public class HessianConverterAdaptor extends ConverterAdaptor<AbstractSerializerFactory> {
	@Override
	public <SRC, DST> AbstractSerializerFactory create(Class<? extends Converter<SRC, DST>> converterClass) {
		final Converter<SRC, DST> converter = getConverter(converterClass);
		final Serializer serializer = new AbstractSerializer() {
			@SuppressWarnings("unchecked")
			@Override
			protected Object writeReplace(Object obj) {
				return converter.serialize((SRC) obj);
			}
		};
		final Deserializer deserializer = new AbstractDeserializer() {
			public Class<?> getType() {
				return converter.getOriginalClass();
			}

			@SuppressWarnings("unchecked")
			public Object readObject(AbstractHessianInput in) throws IOException {
				DST replaced = (DST) in.readObject();
				SRC original = converter.deserialize(replaced);
				return original;
			}
		};
		return new AbstractSerializerFactory() {
			@SuppressWarnings("rawtypes")
			@Override
			public Serializer getSerializer(Class clazz) throws HessianProtocolException {
				return converter.getOriginalClass().isAssignableFrom(clazz) ? serializer : null;
			}

			@SuppressWarnings("rawtypes")
			@Override
			public Deserializer getDeserializer(Class clazz) throws HessianProtocolException {
				return converter.getOriginalClass().isAssignableFrom(clazz) ? deserializer : null;
			}
		};
	}
}
