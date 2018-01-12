package net.butfly.albacore.utils;

import java.lang.reflect.AccessibleObject;

public interface Refs {
	/**
	 * <p>
	 * Collections an array of {@code Object} in to an array of {@code Class} objects. If any of these objects is null, a null element will
	 * be inserted into the array.
	 * </p>
	 *
	 * <p>
	 * This method returns {@code null} for a {@code null} input array.
	 * </p>
	 *
	 * @param array
	 *            an {@code Object} array
	 * @return a {@code Class} array, {@code null} if null array input
	 * @since 2.4
	 */
	static Class<?>[] toClass(final Object... array) {
		if (array == null) return null;
		else if (array.length == 0) return new Class[0];
		final Class<?>[] classes = new Class[array.length];
		for (int i = 0; i < array.length; i++)
			classes[i] = array[i] == null ? null : array[i].getClass();
		return classes;
	}

	@SuppressWarnings("deprecation")
	static boolean accessible(AccessibleObject o) {
		return null == o ? false : o.isAccessible();
	}
}
