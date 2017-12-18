import net.butfly.albacore.utils.Config;
import net.butfly.albacore.utils.Configs;

@Config("test.properties")
public class ConfigTest {
	@Config("item1")
	private String item1;

	public static void main(String... args) {
		ConfigTest test = new ConfigTest();
		System.out.println("item1: " + Configs.get("configs.test.item1"));
		Configs.of(ConfigTest.class);
		System.out.println("item1: " + test.item1);
	}
}
