package net.butfly.albacore.serialize.converter;

import java.io.IOException;
import java.util.ArrayList;

import net.butfly.albacore.utils.Enums;

import com.caucho.hessian.io.AbstractDeserializer;
import com.caucho.hessian.io.AbstractHessianInput;
import com.caucho.hessian.io.AbstractHessianOutput;
import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Deserializer;
import com.caucho.hessian.io.HessianProtocolException;
import com.caucho.hessian.io.Serializer;

@Deprecated
@SuppressWarnings({ "rawtypes", "unchecked" })
public class HessianEnumFactory extends AbstractSerializerFactory {
	@Override
	public Deserializer getDeserializer(Class clazz) throws HessianProtocolException {
		if (Enum.class.isAssignableFrom(clazz)) { return new EnumDeserializer(clazz); }
		return null;
	}

	@Override
	public Serializer getSerializer(Class clazz) throws HessianProtocolException {
		if (Enum.class.isAssignableFrom(clazz)) { return EnumSerializer.instance(); }
		return null;
	}

	public static class EnumSerializer<E extends Enum<E>> implements Serializer {
		private static EnumSerializer INSTANCE = new EnumSerializer();

		public static EnumSerializer instance() {
			return INSTANCE;
		}

		@Override
		public void writeObject(Object obj, AbstractHessianOutput out) throws IOException {
			out.writeInt(Enums.value((Enum<?>) obj));
		}
	}

	public static class EnumDeserializer<E extends Enum<E>> extends AbstractDeserializer implements Deserializer {
		private Class<E> clazz;

		public EnumDeserializer(Class<E> clazz) {
			// hessian/33b[34], hessian/3bb[78]
			this.clazz = clazz;
		}

		@Override
		public Class<?> getType() {
			return this.clazz;
		}

		@Override
		public Object readObject(AbstractHessianInput in) throws IOException {
			return this.readEnum(in);
		}

		@Override
		public Object readList(AbstractHessianInput in, int length) throws IOException {
			if (length >= 0) {
				Enum<?>[] data = new Enum[length];
				in.addRef(data);
				for (int i = 0; i < data.length; i++) {
					data[i] = this.readEnum(in);
				}
				in.readEnd();
				return data;
			} else {
				ArrayList<Enum<?>> list = new ArrayList<Enum<?>>();
				while (!in.isEnd()) {
					list.add(this.readEnum(in));
				}
				in.readEnd();
				Enum<?>[] data = (Enum[]) list.toArray(new Enum[list.size()]);
				in.addRef(data);
				return data;
			}
		}

		@Override
		public Object readLengthList(AbstractHessianInput in, int length) throws IOException {
			Enum<?>[] data = new Enum[length];
			in.addRef(data);
			for (int i = 0; i < data.length; i++) {
				data[i] = this.readEnum(in);
			}
			return data;
		}

		private E readEnum(AbstractHessianInput in) throws IOException {
			return Enums.parse(clazz, (byte) in.readInt());
		}
	}
}
