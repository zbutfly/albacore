package net.butfly.albacore.serder;

import java.lang.reflect.Field;

import net.butfly.albacore.serder.modifier.Property;
import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Reflections;

public interface BeanSerder<DATA> extends Serder<Object, DATA> {
	/**
	 * Mapping the field name based on different ser/der implementation.
	 * 
	 * @param field
	 * @return
	 */
	default String mapping(Field field) {
		Reflections.noneNull("", field);
		if (field.isAnnotationPresent(Property.class)) return field.getAnnotation(Property.class).value();
		else {
			CaseFormat to = mapping();
			return null == to ? field.getName() : CaseFormat.LOWER_CAMEL.to(to, field.getName());
		}
	}

	default CaseFormat mapping() {
		return CaseFormat.NO_CHANGE;
	}

	BeanSerder<DATA> mapping(CaseFormat to);
}
