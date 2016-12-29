package net.butfly.albacore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.key.IdGenerator;
import net.butfly.albacore.utils.key.ObjectIdGenerator;

public class Keys extends Utils {
	private static final Set<Class<? extends IdGenerator>> IDGS = Reflections.getSubClasses(IdGenerator.class);

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K> K key(final Class<K> keyClass) {
		Map<Class<?>, Class<? extends IdGenerator>> m = Instances.fetch(() -> {
			Map<Class<?>, Class<? extends IdGenerator>> map = new HashMap<Class<?>, Class<? extends IdGenerator>>();
			for (Class<? extends IdGenerator> subClass : IDGS) {
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
}
