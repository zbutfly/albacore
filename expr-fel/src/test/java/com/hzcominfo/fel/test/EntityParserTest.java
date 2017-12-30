//package com.hzcominfo.fel.test;
//
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//
//import com.hzcominfo.dataggr.dpc.config.MQL;
//
//import net.butfly.albacore.expr.fel.FelType;
//import net.butfly.albacore.utils.collection.Maps;
//
//public class EntityParserTest {
//
//	public static void main(String[] args) {
//		Map<String, Object> originMap = new HashMap<>();
//		Map<String, Object> dstMap = new HashMap<>();
//		originMap.put("startTime", new Date(1334007167000L));
//		originMap.put("updateTime", new Date(631163535000L));
//		originMap.put("a", "hello");
//		originMap.put("b", "world");
//		originMap.put("c", "!");
//		Map<String, FelType> srcFields = Maps.of("updateTime", FelType.DATE, "c", FelType.STRING);
//		new MQL("concatTest", "concat(dateToString(updateTime, 'yyyyMMdd'), 'asd', c)", srcFields, new String[] { "dateToString",
//				"concat" }).proc(originMap, dstMap);
//		System.out.println(dstMap.toString());
//	}
//}
