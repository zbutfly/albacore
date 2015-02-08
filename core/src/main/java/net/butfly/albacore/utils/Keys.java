package net.butfly.albacore.utils;

import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.utils.async.Task;
import net.butfly.albacore.utils.key.IdGenerator;

public class Keys extends Utils {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K> K key(final Class<K> keyClass) {
		IdGenerator<K> g = Instances.fetch(Instances.fetch(new Task.Callable<Map<Class<?>, Class<? extends IdGenerator>>>() {
			@Override
			public Map<Class<?>, Class<? extends IdGenerator>> call() {
				Map<Class<?>, Class<? extends IdGenerator>> map = new HashMap<Class<?>, Class<? extends IdGenerator>>();
				for (Class<? extends IdGenerator> subClass : Reflections.getSubClasses(IdGenerator.class)) {
					Class<?> pcl = Generics.getGenericParamClass(subClass, IdGenerator.class, "K");
					map.put(pcl, subClass);
				}
				return map;
			}
		}).get(keyClass));
		return g.generate();
	}
}
