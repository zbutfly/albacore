package net.butfly.albacore.utils.key;

public class GetHardwareIdFailed extends RuntimeException {
	private static final long serialVersionUID = -4518004633534465627L;

	GetHardwareIdFailed(String reason) {
		super(reason);
	}

	GetHardwareIdFailed(Exception e) {
		super(e);
	}
}