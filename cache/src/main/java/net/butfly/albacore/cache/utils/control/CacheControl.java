package net.butfly.albacore.cache.utils.control;

public class CacheControl {
	/**
	 * hotel cache开关,关闭时表示即不使用缓存,亦不更新缓存。默认开
	 */
	private static boolean cacheOn = true;
	/**
	 * hotel cache开关,关闭时如果cacheOn开启表示只更新缓存而不使用缓存。
	 */
	private static boolean cacheUseOn = true;
	/**
	 * flight cache开关,关闭时表示即不使用缓存,亦不更新缓存。默认开。
	 */
	private static boolean cacheOnFlight = true;
	/**
	 * flight cache开关,关闭时如果cacheOn开启表示只更新缓存而不使用缓存。
	 */
	private static boolean cacheUseOnFlight = true;

	public static boolean isCacheOnFlight() {
		return cacheOnFlight;
	}

	public static void setCacheOnFlight(boolean cacheOnFlight) {
		CacheControl.cacheOnFlight = cacheOnFlight;
	}

	public static boolean isCacheUseOnFlight() {
		return cacheUseOnFlight;
	}

	public static void setCacheUseOnFlight(boolean cacheUseOnFlight) {
		CacheControl.cacheUseOnFlight = cacheUseOnFlight;
	}

	public static boolean isCacheOn() {
		return cacheOn;
	}

	public static void setCacheOn(boolean cacheOn) {
		CacheControl.cacheOn = cacheOn;
	}

	public static boolean isCacheUseOn() {
		return cacheUseOn;
	}

	public static void setCacheUseOn(boolean cacheUseOn) {
		CacheControl.cacheUseOn = cacheUseOn;
	}

	private CacheControl() {}
}
