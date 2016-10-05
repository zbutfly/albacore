package net.butfly.albacore.serder;

import java.lang.reflect.Field;

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
	default String mapping(String fieldName) {
		return MappingSerder.super.mapping(fieldName);
	}

	@Override
	default BeanSerder<P, D> mapping(CaseFormat to) {
		return this;
	}
}
