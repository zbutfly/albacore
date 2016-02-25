// package net.butfly.albacore.utils.http;
//
// import net.butfly.albacore.exception.SystemException;
// import net.butfly.albacore.support.ContextSupport;
//
// public final class ContextUtils {
// private ContextUtils() {}
//
// @SuppressWarnings("unchecked")
// public static <T extends ContextSupport> T parse(Class<T> enumClass, String
// tag) {
// if (!Enum.class.isAssignableFrom(enumClass)) throw new SystemException("Try
// to parse a non-enum class.");
// T[] values;
// try {
// values = (T[]) enumClass.getMethod("values").invoke(null);
// } catch (Exception e) {
// return null;
// }
// for (T value : values)
// if (tag.equals(value.tag())) return value;
// return null;
// }
// }
