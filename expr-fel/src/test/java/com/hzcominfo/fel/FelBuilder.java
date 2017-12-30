//package com.hzcominfo.fel;
//
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//
//import com.greenpineyu.fel.Expression;
//import com.greenpineyu.fel.FelEngine;
//import com.greenpineyu.fel.FelEngineImpl;
//import com.greenpineyu.fel.context.ArrayCtxImpl;
//import com.greenpineyu.fel.context.FelContext;
//import com.greenpineyu.fel.function.Function;
//
//import net.butfly.albacore.expr.fel.FelFunc;
//import net.butfly.albacore.utils.collection.Maps;
//
//@Deprecated
//public class FelBuilder {
//	private FelEngine engine;
//
//	public FelBuilder() {
//		engine = new FelEngineImpl();
//	}
//
//	public void setFunction(Function fun) {
//		engine.addFun(fun);
//	}
//
//	public void putConext(String key, Object val) {
//		engine.getContext().set(key, val);
//	}
//
//	public Object executeExp(String exp) {
//		return engine.eval(exp);
//	}
//
//	public Expression getCompiledExpression(String exp, FelContext context) {
//		return engine.compile(exp, context);
//	}
//
//	public static void main(String[] args) {
//		Map<String, Date> map = Maps.of("updateTime", new Date());
//		FelBuilder builder = new FelBuilder();
//		builder.setFunction(FelFunc.support("dateToStr"));
//		Map<String, Object> testMap = new HashMap<>();
//		testMap.put("updateTime", new Date(631163535000L));
//		FelContext ctx = new ArrayCtxImpl();
//		ctx.set("map", testMap);
//		Expression expres = builder.getCompiledExpression("dateToString(map.updateTime, 'yyyyMMdd')", ctx);
//
//		map.put("updateTime", new Date(1334007167000L));
//		FelContext ctx1 = new ArrayCtxImpl();
//		ctx1.set("map", map);
//		String eval = (String) expres.eval(ctx1);
//		System.out.println(eval);
//	}
//}
