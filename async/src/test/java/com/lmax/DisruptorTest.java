package com.lmax;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

//https://github.com/jasonk000/examples/tree/master/executors/src/main/java
//http://fasterjava.blogspot.com/2014/09/writing-non-blocking-executor.html
public class DisruptorTest {

	public static class PiJob {
		public double result;
		public int sliceNr;
		public int numIter;

		public void calculatePi() {
			double acc = 0.0;
			for (int i = sliceNr * numIter; i <= ((sliceNr + 1) * numIter - 1); i++) {
				acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
			}
			result = acc;
		}

	}

	public static class PiEventFac implements EventFactory<PiJob> {

		@Override
		public PiJob newInstance() {
			return new PiJob();
		}
	}

	public static class PiEventProcessor implements WorkHandler<PiJob> {
		@Override
		public void onEvent(PiJob event) throws Exception {
			event.calculatePi();
		}
	}

	public static class PiResultReclaimer implements WorkHandler<PiJob> {
		double result;
		public AtomicLong seq = new AtomicLong(0);

		@Override
		public void onEvent(PiJob event) throws Exception {
			result += event.result;
			seq.incrementAndGet();
		}
	}

	@SuppressWarnings("unchecked")
	public long run(int numTH, int numSlice, int numIter) throws InterruptedException {
		PiEventFac fac = new PiEventFac();
		ExecutorService executor = Executors.newCachedThreadPool();
		Disruptor<PiJob> disruptor = new Disruptor<PiJob>(fac, 16384, executor, ProducerType.SINGLE, new BlockingWaitStrategy());
		PiEventProcessor procs[] = new PiEventProcessor[numTH];
		PiResultReclaimer res = new PiResultReclaimer();

		for (int i = 0; i < procs.length; i++) {
			procs[i] = new PiEventProcessor();
		}

		disruptor.handleEventsWithWorkerPool(procs).thenHandleEventsWithWorkerPool(res);

		disruptor.start();

		final RingBuffer<PiJob> ringBuffer = disruptor.getRingBuffer();
		long tim = System.currentTimeMillis();
		for (int i = 0; i < numSlice; i++) {
			final long seq = ringBuffer.next();
			final PiJob piJob = ringBuffer.get(seq);
			piJob.numIter = numIter;
			piJob.sliceNr = i;
			piJob.result = 0;
			ringBuffer.publish(seq);
		}
		while (res.seq.get() < numSlice) {
			Thread.sleep(1);
			// spin
		}
		long timTest = System.currentTimeMillis() - tim;
		System.out.println(numTH + ": tim: " + timTest + " Pi: " + res.result);

		disruptor.shutdown();
		executor.shutdownNow();
		return timTest;
	}

	public static void main(String arg[]) throws InterruptedException {
		final DisruptorTest disruptorTest = new DisruptorTest();
		int numSlice = 1000000;
		int numIter = 100;

		int NUM_CORE = 8;
		String res[] = new String[NUM_CORE];
		for (int i = 1; i <= NUM_CORE; i++) {
			long sum = 0;
			System.out.println("--------------------------");
			for (int ii = 0; ii < 20; ii++) {
				long t = disruptorTest.run(i, numSlice, numIter);
				if (ii >= 10) sum += t;
			}
			res[i - 1] = i + ": " + (sum / 10);
		}
		for (int i = 0; i < res.length; i++) {
			String re = res[i];
			System.out.println(re);
		}
	}
}
