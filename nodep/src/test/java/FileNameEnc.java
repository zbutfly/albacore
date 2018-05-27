import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileNameEnc {
	public static void main(String[] args) throws IOException {
		printEnv("LANG");
		printEnv("LANGUAGE");
		printEnv("LC_CTYPE");
		printEnv("LC_ALL");
		System.out.println("sun.jnu.encoding: " + System.getProperty("sun.jnu.encoding"));
		System.out.println("file.encoding: " + System.getProperty("file.encoding"));
		String dir = "/home/zx/Workspaces/misc/cominfo-spec/开发环境/代码管理";
		String file = "/home/zx/Workspaces/misc/cominfo-spec/开发环境/代码管理/gitignore";
		System.out.println(" EXISTS: " + dir + "<==>" + Files.exists(Paths.get(dir)));
		System.out.println(" EXISTS: " + file + "<==>" + Files.exists(Paths.get(file)));
		System.out.println("Support: " + Charset.availableCharsets());
		int c = 0;
		try (FileInputStream is = new FileInputStream(new String(file.getBytes()));
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader r = new BufferedReader(isr);) {
			String l;
			while (null != (l = r.readLine()))
				c++;
			System.out.println(l);
			System.out.println("Total lines: " + c);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void printEnv(String env) {
		System.out.println(env + ": " + System.getProperty(env));
	}
}
