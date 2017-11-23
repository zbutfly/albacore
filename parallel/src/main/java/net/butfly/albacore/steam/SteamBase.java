package net.butfly.albacore.steam;

import java.util.Objects;

abstract class SteamBase<E, S, SELF extends Steam<E>> implements Steam<E> {
	protected final S impl;

	protected SteamBase(S impl) {
		super();
		this.impl = Objects.requireNonNull(impl);
	}
}
