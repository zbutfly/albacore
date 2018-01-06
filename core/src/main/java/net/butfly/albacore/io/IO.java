package net.butfly.albacore.io;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistical;

public interface IO extends Sizable, Openable, Statistical {
	default <E> Sdream<E> stats(Sdream<E> s) {
		return s.peek(this::stats);
	}
}
