package net.butfly.albacore.calculus.marshall.bson;

import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase;
import com.google.common.base.CaseFormat;

public class UpperCaseWithUnderscoresStrategy extends PropertyNamingStrategyBase {
	private static final long serialVersionUID = 8271159271617770336L;

	@Override
	public String translate(String input) {
		return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, input);
	}
}