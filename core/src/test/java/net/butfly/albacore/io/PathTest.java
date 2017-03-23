package net.butfly.albacore.io;

import java.nio.file.Paths;

public class PathTest {
	public static void main(String[] args) {
		Paths.get("/test/t.txt/").toString();
		Paths.get("").resolve("a.jpg").getName(1).toString();
	}
}
