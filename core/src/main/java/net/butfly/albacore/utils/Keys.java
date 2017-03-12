package net.butfly.albacore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.key.IdGenerator;
import net.butfly.albacore.utils.key.ObjectIdGenerator;

public class Keys extends Utils {
	@SuppressWarnings("rawtypes")
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

	private static final byte[] BYTES_32 = new byte[32];
	static {
		char[] CHARS_32 = new char[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
				'u', 'v', 'w', 'x', 'y', 'z', '1', '2', '3', '4', '5', '6' };
		for (int i = 0; i < CHARS_32.length; i++)
			BYTES_32[i] = (byte) CHARS_32[i];
	}

	/**
	 * convert from {@code UUID} to String[16] use a-z, 1-6
	 * 
	 * @param uuid
	 * @return
	 */
	public static byte[] shortBytes(UUID uuid) {
		byte[] buf = new byte[16];
		long l = uuid.getLeastSignificantBits();
		Long.toBinaryString(31);
		long vvl = uuid.getLeastSignificantBits() & 127L;
		for (int i = 0; i < 8; i++) {
			buf[i] = BYTES_32[(int) (l & 31)];
			l = l >> 8;
		}
		l = uuid.getMostSignificantBits();
		for (int i = 8; i < 16; i++) {
			buf[i] = BYTES_32[(int) (l & 31)];
			l = l >> 8;
		}
		return buf;
	}

	public static String shortString(UUID uuid) {
		return new String(shortBytes(uuid));
	}

	public static void main(String... args) {
		UUID uuid = UUID.randomUUID();
		System.out.println(uuid.toString().replaceAll("-", ""));
		System.out.println(" ==> ");
		System.out.println(shortString(uuid));
	}
}
