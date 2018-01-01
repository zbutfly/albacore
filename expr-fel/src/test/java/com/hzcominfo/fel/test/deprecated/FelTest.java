package com.hzcominfo.fel.test.deprecated;
//package com.hzcominfo.fel.test;
//
//import java.io.IOException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import com.greenpineyu.fel.FelEngine;
//import com.greenpineyu.fel.FelEngineImpl;
//import com.greenpineyu.fel.context.AbstractContext;
//import com.greenpineyu.fel.context.FelContext;
//import com.hzcominfo.dataggr.dpc.config.MQL;
//
//public class FelTest {
//	public static void main(String[] args) {
//		FelEngine engine = new FelEngineImpl();
//		String testString = "helloworld";
//		// String subStr = "";
//		FelContext ctx = engine.getContext();
//		ctx.set("testString", (Object) testString);
//		Object eval = (String) engine.eval("testString.substr(0,5)+testString");
//		System.out.println(eval);
//		FelTest fel = new FelTest();
//		fel.parseQuery("field3=field1+field2");
//		fel.parseQuery("fieldDate=$('net.butfly.albacore.expr.fel.test.FelTest').castDateToString(field5, 'yyyyMMdd')");
//		FelContext j = new AbstractContext() {
//			@Override
//			public Object get(String name) {
//				if ("天气".equals(name)) { return "晴"; }
//				if ("温度".equals(name)) { return 25; }
//				return null;
//			}
//		};
//		FelEngine fel1 = new FelEngineImpl(ctx);
//		Object eval1 = fel1.eval("'天气:'+天气+';温度:'+温度");
//		System.out.println(eval1);
//
//		// fel.testMQLParser();
//	}
//
//	public Object getExecResult(String expression, String[] srcField, Map<String, Object> temp) {
//		FelEngine engine = new FelEngineImpl();
//		FelContext ctx = engine.getContext();
//		for (String field : srcField) {
//			ctx.set(field, temp.get(field));
//		}
//		Object eval = engine.eval(expression);
//		return eval;
//	}
//
//	public void parseQuery(String query) {
//		String[] querys = query.split("=");
//		String dstField = querys[0];
//		String expression = querys[1];
//		Map<String, Object> temp = new HashMap<String, Object>();
//		temp.put("field1", "hello");
//		temp.put("field2", "world");
//		temp.put("field5", new Date());
//		String[] fields = new String[] { "field1", "field2", "field5" };
//		Object eval = getExecResult(expression, fields, temp);
//		System.out.println(eval);
//	}
//
//	public void testDate() {
//		SimpleDateFormat sdf = new SimpleDateFormat("");
//		sdf.format(new Date());
//	}
//
//	public static String castDateToString(Date date, String format) {
//		return new SimpleDateFormat(format).format(date);
//	}
//
//	public void testMQLParser() throws IOException {
//		Map<String, Object> originMap = new HashMap<>();
//		Map<String, Object> dstMap = new HashMap<>();
//		originMap.put("startTime", new Date(1334007167000L));
//		originMap.put("updateTime", new Date(631163535000L));
//		originMap.put("a", "hello");
//		originMap.put("b", "world");
//		originMap.put("c", "!");
//		List<MQL> expressions = MQL.load("E:\\test\\test.txt");
//		expressions.forEach(e -> e.proc(originMap, dstMap));
//		System.out.println(dstMap.toString());
//	}
//}
