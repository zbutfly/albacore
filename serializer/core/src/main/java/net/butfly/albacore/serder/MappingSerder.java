package net.butfly.albacore.serder;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Reflections;

public interface MappingSerder<P, D> extends Serder<P, D> {
	/**
	 * Mapping the field name based on different ser/der implementation.
	 * 
	 * @param fieldName
	 * @return
	 */
	default String mapping(String fieldName) {
		Reflections.noneNull("", fieldName);
		CaseFormat to = mapping();
		return null == to ? fieldName : CaseFormat.LOWER_CAMEL.to(to, fieldName);
	}

	default CaseFormat mapping() {
		return CaseFormat.NO_CHANGE;
	}

	default MappingSerder<P, D> mapping(CaseFormat to) {
		return this;
	}
}
