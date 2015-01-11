package net.butfly.albacore.exception;

import javax.validation.ConstraintViolation;

public class ValidateException extends BusinessException {
	private static final long serialVersionUID = -4154151928102997805L;
	private ConstraintViolation<?>[] violations;

	public ConstraintViolation<?>[] getViolations() {
		return violations;
	}

	public ValidateException(ConstraintViolation<?>[] violations) {
		super(Exceptions.VALID_CODE, generateMessage(violations));
		this.violations = violations;
	}

	private static String generateMessage(ConstraintViolation<?>[] violations) {
		return null;
	}
}
