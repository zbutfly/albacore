package net.butfly.albacore.paral.split;

import static java.util.Spliterator.CONCURRENT;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SIZED;
import static java.util.Spliterator.SORTED;
import static java.util.Spliterator.SUBSIZED;

public interface SplitChars {
	final int ALL = SORTED | DISTINCT | SUBSIZED | ORDERED | SIZED | NONNULL | CONCURRENT | IMMUTABLE;
	final int NON_ALL = ~ALL;
	// never merge
	final int NON_SORTED = ~SORTED;
	final int NON_DISTINCT = ~DISTINCT;
	// and merge
	final int NON_SUBSIZED = ~SUBSIZED;
	final int NON_ORDERED = ~ORDERED;
	final int NON_SIZED = ~SIZED;
	final int NON_NONNULL = ~NONNULL;
	final int NON_CONCURRENT = ~CONCURRENT;
	// or merge
	final int NON_IMMUTABLE = ~IMMUTABLE;

	static boolean has(int ch, int bit) {
		return (ch & bit) == ch;
	}

	static int merge(int ch1, int ch2) {
		int and = ch1 & ch2 & SUBSIZED & ORDERED & SIZED & NONNULL & CONCURRENT;
		int or = (ch1 | ch2) & IMMUTABLE;
		int non = NON_SORTED | NON_DISTINCT;
		return ch1 & ch2 & NON_ALL | and | or | non;
	}
}