package net.butfly.albacore.utils;

import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.async.Task;
import net.butfly.albacore.utils.key.IdGenerator;
import net.butfly.albacore.utils.key.ObjectIdGenerator;

public class Keys extends Utils {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K> K key(final Class<K> keyClass) {
		Class<? extends IdGenerator> genClass = Instances
				.fetch(new Task.Callable<Map<Class<?>, Class<? extends IdGenerator>>>() {
					@Override
					public Map<Class<?>, Class<? extends IdGenerator>> call() {
						Map<Class<?>, Class<? extends IdGenerator>> map = new HashMap<Class<?>, Class<? extends IdGenerator>>();
						for (Class<? extends IdGenerator> subClass : Reflections.getSubClasses(IdGenerator.class)) {
							Class<?> pcl = Generics.getGenericParamClass(subClass, IdGenerator.class, "K");
							map.put(pcl, subClass);
						}
						return map;
					}
				}).get(keyClass);
		if (null == genClass) throw new SystemException("", "Could not found any key generator class.");
		IdGenerator<K> g = Instances.fetch(genClass);
		return g.generate();
	}

	@Deprecated
	public static String objectId() {
		return Instances.fetch(ObjectIdGenerator.class).generate();
	}
}
