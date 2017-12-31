package com.hzcominfo.fel.test;

import java.util.List;
import java.util.concurrent.Callable;

import net.butfly.albacore.expr.fel.FelEngineer;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

public class FelEngTest {
	public static void main(String[] args) {
		String caseResult = FelEngineer.eval("case(type, '001', phone, '002', qq, null)", //
				Maps.of("type", "001", //
						"phone", "057188338822", //
						"qq", "2933094"));
		System.out.println(caseResult);
		String regex = "\\d*";
		System.out.println("reg: /" + regex + "/");
		boolean actully = FelEngineer.eval("match('001', reg)", Maps.of("reg", regex));
		System.out.println("expect: " + "001".matches(regex) + ", actually: " + actully);

	}

	public static void testParalsEval() {
		List<Callable<Boolean>> fs = Colls.list();
		for (int i = 0; i < 1000; i++)
			fs.add(() -> FelEngineer.eval("match('001', reg)", Maps.of("reg", "\\d*")));
		List<Boolean> rs = Exeter.of().join(fs);
		System.err.println(rs);

	}
}
