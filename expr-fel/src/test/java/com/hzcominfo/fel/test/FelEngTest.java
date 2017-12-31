package com.hzcominfo.fel.test;

import java.util.regex.Pattern;

import net.butfly.albacore.expr.fel.FelEngineer;
import net.butfly.albacore.utils.collection.Maps;

public class FelEngTest {
	public static void main(String[] args) {
		Object o = FelEngineer.eval("case(type, '001', phone, '002', qq, null)", //
				Maps.of("type", "001", //
						"phone", "057188338822", //
						"qq", "2933094"));
		System.out.println(o);
		Pattern p = Pattern.compile("\\d*");
		boolean expect = p.matcher("001").find();
		System.out.println("expect: " + expect);
		o = FelEngineer.eval("match('001', p)", Maps.of("p", p));
		System.out.println("actually: " + o);
	}
}
