package net.butfly.albacore.exception;

import java.io.PrintStream;

public class AggregaedException extends SystemException {
	private static final long serialVersionUID = 2170150324562781786L;
	private Throwable[] causes;

	public AggregaedException(String code, String message, Throwable... causes) {
		super(code, message);
		this.causes = causes;
	}

	public Throwable[] getTargetCauses() {
		return causes;
	}

	@Override
	public void printStackTrace(PrintStream s) {
		super.printStackTrace();
		s.println(this.getMessage());
		s.println("Caused by multiple causes: <==");
		for (int i = 0; i < causes.length; i++) {
			s.println("Cause[" + i + "]");
			causes[i].printStackTrace(s);
		}
		s.println("Causes end. ==>");
	}
}
