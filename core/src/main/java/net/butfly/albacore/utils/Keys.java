package net.butfly.albacore.utils;

import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.key.IdGenerator;
import net.butfly.albacore.utils.key.ObjectIdGenerator;

public class Keys extends Utils {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K> K key(final Class<K> keyClass) {
		Map<Class<?>, Class<? extends IdGenerator>> m = Instances.fetch(() -> {
			Map<Class<?>, Class<? extends IdGenerator>> map = new HashMap<Class<?>, Class<? extends IdGenerator>>();
			for (Class<? extends IdGenerator> subClass : Reflections.getSubClasses(IdGenerator.class)) {
				Class<?> pcl = Generics.resolveGenericParameter(subClass, IdGenerator.class, "K");
				map.put(pcl, subClass);
			}
			return map;
		}, Map.class);
		Class<? extends IdGenerator> genClass = m.get(keyClass);
		if (null == genClass) throw new SystemException("", "Could not found any key generator class.");
		IdGenerator<K> g = Instances.construct(genClass);
		return g.generate();
	}

	@Deprecated
	public static String objectId() {
		return Instances.construct(ObjectIdGenerator.class).generate();
	}

	public static byte[] bytes(long v0, long... vs) {
		byte[] buf = new byte[8 * (vs.length + 1)];
		for (int i = 7; i >= 0; i--) {
			buf[i] = (byte) (v0 & 255);
			v0 >>= 8;
		}
		for (int j = 8, j0 = 0; j < buf.length; j += 8, j0++) {
			long v = vs[j0];
			for (int i = 7; i >= 0; i--) {
				buf[j + i] = (byte) (v & 255);
				v >>= 8;
			}
		}
		return buf;

	}
}
