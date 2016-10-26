package net.butfly.albacore.serder;

import java.lang.reflect.Field;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.modifier.Property;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Reflections;

public interface BeanSerder<P, D> extends MappingSerder<P, D> {
	/**
	 * Mapping the field name based on different ser/der implementation.
	 * 
	 * @param field
	 * @return
	 */
	default String mapping(Field field) {
		Reflections.noneNull("", field);
		if (field.isAnnotationPresent(Property.class)) return field.getAnnotation(Property.class).value();
		else return MappingSerder.super.mapping(field.getName());
	}

	@Override
	default BeanSerder<P, D> mapping(CaseFormat to) {
		return this;
	}

	default <R> BeanSerder<P, R> then(BeanSerder<D, R> next, TypeToken<D> data) {
		return new BeanSerder<P, R>() {
			private static final long serialVersionUID = 3913516237104388274L;

			@Override
			public <T extends P> R ser(T from) {
				return next.ser(BeanSerder.this.ser(from));
			}

			@Override
			public <T extends P> T der(R from, TypeToken<T> to) {
				return BeanSerder.this.der(next.der(from, data), to);
			}
		};
	}
}
