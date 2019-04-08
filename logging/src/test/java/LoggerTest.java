import net.butfly.albacore.utils.logger.Logger;

public class LoggerTest {
	private static Logger logger = Logger.getLogger(LoggerTest.class);

	public static void main(String[] args) {
		logger.error("info");
	}
}
