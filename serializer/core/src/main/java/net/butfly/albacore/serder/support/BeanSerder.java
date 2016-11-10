package net.butfly.albacore.serder.support;

import java.lang.reflect.Field;

import net.butfly.albacore.serder.Serder;
import net.butfly.albacore.serder.modifier.Property;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.Reflections;

public interface BeanSerder<P, D> extends Serder<P, D> {
	@Override
	default String mappingFieldName(String fieldName) {
		return mappingField(Reflections.getDeclaredField(this.getClass(), fieldName));
	}

	@Override
	default String unmappingFieldName(Class<?> objectClass, String propName) {
		return unmappingField(objectClass, propName).getName();
	}

	default String mappingField(Field field) {
		Reflections.noneNull("", field);
		if (field.isAnnotationPresent(Property.class)) return field.getAnnotation(Property.class).value();
		else return field.getName();
	}

	default Field unmappingField(Class<?> objectClass, String propName) {
		if (null == objectClass) return null;
		Reflections.noneNull("", propName);
		for (Field field : Reflections.getDeclaredFields(objectClass.getClass())) {
			if (field.isAnnotationPresent(Property.class) && propName.equals(field.getAnnotation(Property.class).value())) return field;
			if (propName.equals(field.getName())) return field;
		}
		return null;
	}

	default <R> BeanSerder<P, R> then(BeanSerder<D, R> next, Class<D> clazz) {
		return new BeanSerder<P, R>() {
			private static final long serialVersionUID = 3913516237104388274L;

			@Override
			public R ser(P from) {
				return next.ser(BeanSerder.this.ser(from));
			}

			@Override
			public <T extends P> T der(R from, Class<T> to) {
				return BeanSerder.this.der(next.der(from, clazz), to);
			}
		};
	}

	default BeanSerder<P, D> mapping(CaseFormat from, CaseFormat to) {
		Objects.noneNull(from, to);
		return new BeanSerder<P, D>() {
			private static final long serialVersionUID = 6764476365699548092L;

			@Override
			public D ser(P from) {
				return BeanSerder.this.ser(from);
			}

			@Override
			public <T extends P> T der(D from, Class<T> to) {
				return BeanSerder.this.der(from, to);
			}

			@Override
			public Object[] der(D from, Class<?>... tos) {
				return BeanSerder.this.der(from, tos);
			}

			@Override
			public String mappingFieldName(String fieldName) {
				return from.to(to, fieldName);
			}

			@Override
			public String unmappingFieldName(Class<?> objectClass, String propName) {
				return to.to(from, propName);
			}
		};
	}
}
