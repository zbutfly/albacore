package net.butfly.albacore.utils;

public final class Systems extends Utils {
	public static Class<?> getMainClass() {
		try {
			return Class.forName(System.getProperty("sun.java.command"));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public static boolean isDebug() {
		return Boolean.parseBoolean(System.getProperty("albacore.debug"));
	}
}
