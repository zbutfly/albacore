package net.butfly.albacore.utils;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;

import net.butfly.albacore.exception.ValidateException;

public final class Validator extends UtilsBase {
	private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();

	public static void validate(Object dto) throws ValidateException {
		Set<ConstraintViolation<Object>> cvs = factory.getValidator().validate(dto);
		if (0 == cvs.size()) { return; }
		throw new ValidateException(cvs.toArray(new ConstraintViolation[cvs.size()]));
	}
}
