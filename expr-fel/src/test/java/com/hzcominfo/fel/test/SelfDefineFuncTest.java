package com.hzcominfo.fel.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.FelEngine;
import com.greenpineyu.fel.FelEngineImpl;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.function.CommonFunction;
import com.greenpineyu.fel.function.Function;

public class SelfDefineFuncTest {
	public static void main(String[] args) {
		Function fun = new CommonFunction() {
			@Override
			public String getName() {
				return "hello";
			}

			@Override
			public Object call(Object[] args) {
				Object msg = null;
				if (args != null && args.length > 0) {
					msg = args[0];
				}
				return ((String[]) msg)[0];
			}
		};
		Function fun1 = new CommonFunction() {
			@Override
			public String getName() {
				return "hello1";
			}

			@Override
			public Object call(Object[] args) {
				Object msg = null;
				if (args != null && args.length > 0) {
					msg = args[0];
				}
				return ((String[]) msg)[1];
			}
		};
		Function c = new CommonFunction() {
			@Override
			public String getName() {
				return "concat";
			}

			@Override
			public Object call(Object[] args) {
				String result = "";
				for (Object argument : args) {
					result += argument.toString();
				}
				return result;
			}
		};
		Function d2s = new CommonFunction() {
			@Override
			public String getName() {
				return "dateToString";
			}

			@Override
			public Object call(Object[] args) {
				Date srcField = (Date) args[0];
				String format = (String) args[1];
				SimpleDateFormat sdf = new SimpleDateFormat(format);
				String dstField = sdf.format(srcField);
				return dstField;
			}
		};
		FelEngine e = new FelEngineImpl();
		e.addFun(fun);
		e.addFun(fun1);
		e.addFun(d2s);
		e.addFun(c);
		FelContext ctx = e.getContext();
		ctx.set("updateTime", new Date());
		Expression expression = e.compile("concat(dateToString(updateTime, 'yyyyMMdd'), 'asd')", ctx);
		System.out.println("hello++++++++ " + expression.eval(ctx));
	}

}
