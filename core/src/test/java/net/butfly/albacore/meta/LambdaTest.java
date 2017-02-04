//package net.butfly.albacore.meta;
//
//import java.io.Serializable;
//import java.lang.invoke.CallSite;
//import java.lang.invoke.LambdaMetafactory;
//import java.lang.invoke.MethodHandle;
//import java.lang.invoke.MethodHandles;
//import java.lang.invoke.MethodType;
//import java.util.function.Predicate;
//
//public class LambdaTest {
//
//	public static void main(String[] args) {
//		LambdaMetafactory.altMetafactory(MethodHandles.lookup(), "test", invokedType, args);
//		MethodType predicateMT = MethodType.methodType(boolean.class, Object.class);
//		// Alt metafactory, serializable marker, no FLAG_SERIALIZABLE: not
//		// serializable
//		MethodHandle fooMH = MethodHandles.lookup().findStatic(LambdaTest.class, "foo", predicateMT);
//		cs = LambdaMetafactory.altMetafactory(MethodHandles.lookup(), "test", MethodType.methodType(Predicate.class), predicateMT, fooMH,
//				MethodType.methodType(boolean.class, String.class), LambdaMetafactory.FLAG_SERIALIZABLE);
//		assertNotSerial((Predicate<String>) cs.getTarget().invokeExact(), fooAsserter);
//	}
//
//	Predicate<String> foo(String t) {
//		return (Predicate<String> & Serializable) s -> s.equals(t);
//	}
//}
