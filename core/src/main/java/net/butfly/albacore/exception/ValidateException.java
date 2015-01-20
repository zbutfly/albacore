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
		if (null == violations || violations.length == 0) return null;
		StringBuilder sb = new StringBuilder();
		for (ConstraintViolation<?> cv : violations) {
			sb.append("[参数对象:" + cv.getRootBeanClass() + ",");
			sb.append("属性:" + cv.getPropertyPath() + ",");
			sb.append("错误信息:" + cv.getMessage() + "]\n");
		}
		return sb.toString();
	}
}
