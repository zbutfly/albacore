package net.butfly.albacore.calculus;

import java.lang.annotation.Repeatable;

import net.butfly.albacore.calculus.factor.Factor;

public @interface Relations {
	Relation[] value();

	@Repeatable(Relations.class)
	public @interface Relation {
		Class<? extends Factor<?>> referredMapper();

		String referredField();

		Class<? extends Factor<?>> masterMapper();

		String masterField();
	}
}
