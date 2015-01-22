package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.ObjectUtils;

public abstract class Entity<K extends Serializable> extends Bean<AbstractEntity<K>> implements AbstractEntity<K> {
	private static final long serialVersionUID = -1L;
	protected K id;

	@Override
	public K getId() {
		return id;
	}

	@Override
	public void setId(K id) {
		this.id = id;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compareTo(AbstractEntity other) {
		return ObjectUtils.compare(this.id, other.getId());
	}

//	private static final Map<Class<? extends AbstractEntity<?>>, Class<? extends Serializable>> KEY_TYPE_POOL = new ConcurrentHashMap<Class<? extends AbstractEntity<?>>, Class<? extends Serializable>>();
//
//	@SuppressWarnings("unchecked")
//	public static Class<?> getKeyClass(Class<? extends AbstractEntity<?>> entityClass) {
//		try {
//			Class<? extends Serializable> keyType = KEY_TYPE_POOL.get(entityClass);
//			if (null == keyType) {
//				keyType = (Class<? extends Serializable>) GenericUtils.getGenericParamClass(entityClass, AbstractEntity.class,
//						"K");
//				KEY_TYPE_POOL.put(entityClass, keyType);
//			}
//			return keyType;
//		} catch (SecurityException e) {
//			throw new SystemException("", e);
//		} catch (Throwable e) {
//			throw new SystemException("", e);
//		}
//	}
//
//	@SuppressWarnings("unchecked")
//	public static <K extends Serializable> K[] getKeyBuffer(Class<? extends AbstractEntity<K>> entityClass, int length) {
//		return (K[]) Array.newInstance(getKeyClass(entityClass), length);
//	}
//
//	public static <K extends Serializable> K[] getKeyBuffer(Class<? extends AbstractEntity<K>> entityClass) {
//		return getKeyBuffer(entityClass, 0);
//	}
}
