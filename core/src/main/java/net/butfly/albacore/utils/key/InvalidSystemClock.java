package net.butfly.albacore.utils.key;

public class InvalidSystemClock extends RuntimeException {
	private static final long serialVersionUID = -8072785244821967950L;

	public InvalidSystemClock(String message) {
		super(message);
	}
}