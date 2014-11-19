//package net.butfly.albacore.test.support;
//
//import java.io.IOException;
//import java.io.InvalidObjectException;
//import java.io.ObjectInputStream;
//import java.io.ObjectStreamException;
//import java.io.Serializable;
//
//public class EnumBase<V extends Serializable> implements Serializable, Comparable<EnumBase<V>> {
//	private static final long serialVersionUID = -8677135938419855862L;
//	private final int ordinal;
//	private final String name;
//	private final V value;
//
//	protected EnumBase(int ordinal) {
//		this(ordinal, null);
//	}
//
//	protected EnumBase(int ordinal, String name) {
//		this(ordinal, null, null);
//	}
//
//	protected EnumBase(int ordinal, String name, V value) {
//		this.ordinal = ordinal;
//		this.name = name;
//		this.value = value;
//	}
//
//	public final int ordinal() {
//		return this.ordinal;
//	}
//
//	public final String name() {
//		return this.name;
//	}
//
//	public final V value() {
//		return this.value;
//	}
//
//	@Override
//	public final String toString() {
//		return null == this.value ? null : this.value.toString();
//	}
//
//	@Override
//	public final int compareTo(EnumBase<V> e) {
//		return null == e ? 1 : this.ordinal - e.ordinal;
//	}
//
//	@SuppressWarnings("rawtypes")
//	public final boolean equals(Object other) {
//		if (null == other) return false;
//		if (!EnumBase.class.isAssignableFrom(other.getClass())) return false;
//		return this.ordinal == ((EnumBase) other).ordinal;
//	}
//
//	public final int hashCode() {
//		return super.hashCode();
//	}
//
//	protected final Object clone() throws CloneNotSupportedException {
//		throw new CloneNotSupportedException();
//	}
//
//	public static <T extends EnumBase<T>> T valueOf(Class<T> enumType, String name) {
//		if (name == null) throw new NullPointerException("Name is null");
//		throw new IllegalArgumentException("No enum constant " + enumType.getCanonicalName() + "." + name);
//	}
//
//	protected final void finalize() {}
//
//	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//		throw new InvalidObjectException("can't deserialize enum");
//	}
//
//	@SuppressWarnings("unused")
//	private void readObjectNoData() throws ObjectStreamException {
//		throw new InvalidObjectException("can't deserialize enum");
//	}
// }
