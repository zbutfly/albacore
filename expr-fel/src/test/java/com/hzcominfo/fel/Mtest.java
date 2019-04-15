package com.hzcominfo.fel;

import java.io.IOException;

import net.butfly.albacore.expr.Engine;

public class Mtest {
	public static void main(String[] args) throws IOException {
		// if (args.length < 1) throw new RuntimeException("未输入正确来源表参数");
//		String a = "dsf\ndff\tvf".replaceAll("\n", "");
		String exp = "replace(\"李是\n多少\",\"\\n\",\"\")";
		String d = Engine.eval(exp);
	}
}
