package com.hzcominfo.fel.test;

import static net.butfly.albacore.expr.Engine.eval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

public class FelTest {
	public static void main(String[] args) {
		// System.out.println((Object) eval("123.345 + 123", null));
		// System.out.println((Object) eval("concat('123', 123.345)", null));
		// System.out.println((Object) eval("concat('123', 123.345) + 123", null));
		// System.out.println((Object) eval("strToDate('20180812', 'yyyyMMdd')", null));
		// System.out.println((Object) eval("dateToStr(strToDate('20180812', 'yyyyMMdd'), 'yyyyMMddhhmm')", null));
		//
		// String caseResult = eval("case(type, '001', phone, '002', qq, null)", //
		// Maps.of("type", "001", //
		// "phone", "057188338822", //
		// "qq", "2933094"));
		// System.out.println(caseResult);
		// String regex = "\\d*";
		// System.out.println("reg: /" + regex + "/");
		// boolean actully = eval("match('001', reg)", Maps.of("reg", regex));
		// System.out.println("RegExp matching test, expect: " + "001".matches(regex) + ", actually: " + actully);

		System.out.println();
		System.err.println("======Performance testing.");
		System.out.println();
		int n = 100000;
		perf("concat(s, ',', d)", n);
		perf("s + ',' + d", n);
	}

	public static void testParalsEval() {
		List<Callable<Boolean>> fs = Colls.list();
		for (int i = 0; i < 1000; i++)
			fs.add(() -> eval("match('001', reg)", Maps.of("reg", "\\d*")));
		List<Boolean> rs = Exeter.of().join(fs);
		System.err.println(rs);
	}

	private static void perf(String expr, int n) {
		Map<String, Object> map = Maps.of("s", Double.toString(Math.random() * 100), "d", Math.random() * 100);
		System.out.println((Object) eval(expr, map));
		long now = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			map.put("s", Double.toString(Math.random() * 100));
			map.put("d", Math.random() * 100);
			eval(expr, map);
		}
		System.err.println("Performance of \"" + expr + "\" for [" + n + " times]: " + (System.currentTimeMillis() - now) + " ms.");
	}
}
