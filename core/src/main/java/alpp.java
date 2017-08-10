import net.butfly.albacore.utils.JVMRunning;

public final class alpp {
	public static void main(String... args) throws Throwable {
		JVMRunning.current().args(args).fork(Boolean.parseBoolean(System.getProperty("albacore.app.daemon", "false"))).unwrap();
	}
}
