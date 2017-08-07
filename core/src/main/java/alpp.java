import net.butfly.albacore.utils.JVMRunning;

public final class alpp {
	public static void main(String... args) throws Throwable {
		JVMRunning r = new JVMRunning(alpp.class, args);
		r.fork(Boolean.parseBoolean(System.getProperty("albacore.app.daemon", "false")));
		r.unwrap();
	}
}
