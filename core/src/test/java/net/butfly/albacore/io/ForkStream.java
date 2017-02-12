//package net.butfly.albacore.io;
//
//import java.util.concurrent.ExecutionException;
//import java.util.stream.IntStream;
//
//public class ForkStream {
//	public static void main(String[] args) throws InterruptedException, ExecutionException {
//		IntStream s = IntStream.range(0, 100000).parallel().map(i -> {
//			return i + 1;
//		});
//		System.err.println(IO.executor().submit(() -> s.sum()).get());
//		System.err.println(IntStream.range(0, 100000).parallel().map(i -> {
//			return i + 2;
//		}).sum());
//	}
//}
