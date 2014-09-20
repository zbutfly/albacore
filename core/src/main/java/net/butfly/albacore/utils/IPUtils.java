package net.butfly.albacore.utils;

/**
 * IP地址相关的工具类，封装常用的IP工具方法
 * 
 * 项目名称：base 类名称：IPUtils 类描述： 创建人：HO274541 创建时间：2010-8-17 下午03:44:55
 * 修改人：HO274541 修改时间：2010-8-17 下午03:44:55
 * 
 * @version *
 */
public class IPUtils {
	// 非法IP地址
	public static final String INVALID_IP = "0.0.0.0";
	// 未知主机名常量
	public static final String UNKNOWN_HOST = "";

	private IPUtils() {}

	/**
	 * 根据主机名得到IP地址字符串。
	 * 
	 * @param hostName
	 *            要查找地址的主机名
	 * @return 对应主机的IP地址，主机名未知或者非法时返回INVALID_IP。
	 */
	/*
	 * private static String getByName(String hostName) { try { InetAddress inet
	 * = InetAddress.getByName(hostName); return inet.getHostAddress(); } catch
	 * (UnknownHostException e) { return INVALID_IP; } }
	 */

	/**
	 * 根据IP地址得到主机名。
	 * 
	 * @param ip
	 *            要查找主界面的IP地址
	 * @return 对应IP的主机名，IP地址未知时返回UNKNOWN_HOST，IP地址未知也可能是网络问题造成的。
	 */
	/*
	 * private static String getHostName(String ip) { try { InetAddress inet =
	 * InetAddress.getByName(ip); return inet.getHostName(); } catch
	 * (UnknownHostException e) { return UNKNOWN_HOST; } }
	 */

	/**
	 * 将字符串形式的ip地址转换为BigInteger
	 * 
	 * stringToBigInt(这里用一句话描述这个方法的作用)
	 * 
	 * @param name
	 * @param @return 设定文件
	 * @return String DOM对象
	 * @Exception 异常对象
	 * @since CodingExample　Ver(编码范例查看) 1.1
	 */
	/*
	 * private static BigInteger stringToBigInt(String ipInString) { ipInString
	 * = ipInString.replace(" ", ""); byte[] bytes; if
	 * (ipInString.contains(":")) { bytes = ipv6ToBytes(ipInString); }else {
	 * bytes = ipv4ToBytes(ipInString); } return new BigInteger(bytes); }
	 */

	/**
	 * 将整数形式的ip地址转换为字符串形式
	 * 
	 * bigIntToString(这里用一句话描述这个方法的作用)
	 * 
	 * @param name
	 * @param @return 设定文件
	 * @return String DOM对象
	 * @Exception 异常对象
	 * @since CodingExample　Ver(编码范例查看) 1.1
	 */
	/*
	 * private static String bigIntToString(BigInteger ipInBigInt) { byte[]
	 * bytes = ipInBigInt.toByteArray(); byte[] unsignedBytes =
	 * Arrays.copyOfRange(bytes, 1, bytes.length); // 去除符号位 try { String ip =
	 * InetAddress.getByAddress(unsignedBytes).toString(); return
	 * ip.substring(ip.indexOf('/') + 1).trim(); } catch (UnknownHostException
	 * e) { throw new RuntimeException(e); } }
	 */

	/**
	 * ipv6地址转有符号byte[17]
	 * 
	 * ipv6ToBytes(这里用一句话描述这个方法的作用)
	 * 
	 * @param name
	 * @param @return 设定文件
	 * @return String DOM对象
	 * @Exception 异常对象
	 * @since CodingExample　Ver(编码范例查看) 1.1
	 */
	/*
	 * private static byte[] ipv6ToBytes(String ipv6) { byte[] ret = new
	 * byte[17]; ret[0] = 0; int ib = 16; boolean comFlag = false;// ipv4混合模式标记
	 * if (ipv6.startsWith(":")) { // 去掉开头的冒号 ipv6 = ipv6.substring(1); }
	 * 
	 * String groups[] = ipv6.split(":"); for (int ig = groups.length - 1; ig >
	 * -1; ig--) {// 反向扫描 if (groups[ig].contains(".")) { // 出现ipv4混合模式 byte[]
	 * temp = ipv4ToBytes(groups[ig]); ret[ib--] = temp[4]; ret[ib--] = temp[3];
	 * ret[ib--] = temp[2]; ret[ib--] = temp[1]; comFlag = true; } else if
	 * ("".equals(groups[ig])) { // 出现零长度压缩,计算缺少的组数 int zlg = 9 - (groups.length
	 * + (comFlag ? 1 : 0)); while (zlg-- > 0) {// 将这些组置0 ret[ib--] = 0;
	 * ret[ib--] = 0; } } else { int temp = Integer.parseInt(groups[ig], 16);
	 * ret[ib--] = (byte) temp; ret[ib--] = (byte) (temp >> 8); } } return ret;
	 * }
	 */

	/**
	 * ipv4地址转有符号byte[5]
	 * 
	 * ipv4ToBytes(这里用一句话描述这个方法的作用)
	 * 
	 * @param name
	 * @param @return 设定文件
	 * @return String DOM对象
	 * @Exception 异常对象
	 * @since CodingExample　Ver(编码范例查看) 1.1
	 */
	/*
	 * private static byte[] ipv4ToBytes(String ipv4) { byte[] ret = new
	 * byte[5]; ret[0] = 0; // 先找到IP地址字符串中.的位置 int position1 =
	 * ipv4.indexOf("."); int position2 = ipv4.indexOf(".", position1 + 1); int
	 * position3 = ipv4.indexOf(".", position2 + 1); // 将每个.之间的字符串转换成整型 ret[1] =
	 * (byte) Integer.parseInt(ipv4.substring(0, position1)); ret[2] = (byte)
	 * Integer.parseInt(ipv4.substring(position1 + 1, position2)); ret[3] =
	 * (byte) Integer.parseInt(ipv4.substring(position2 + 1, position3)); ret[4]
	 * = (byte) Integer.parseInt(ipv4.substring(position3 + 1)); return ret; }
	 */

	/*
	 * private static List<String> getPhysicalAddress() { Process p = null; //
	 * 物理网卡列表 List<String> address = new ArrayList<String>();
	 * 
	 * try { // 执行ipconfig /all命令 p = new ProcessBuilder("ipconfig",
	 * "/all").start(); } catch (IOException e) { return address; } byte[] b =
	 * new byte[1024]; StringBuffer sb = new StringBuffer(); // 读取进程输出值
	 * InputStream in = p.getInputStream(); try { while (in.read(b) > 0) {
	 * sb.append(new String(b)); } } catch (IOException e1) {
	 * e1.printStackTrace(); } finally { try { in.close(); } catch (IOException
	 * e2) { e2.printStackTrace(); } } // 以下分析输出值，得到物理网卡 String rtValue =
	 * sb.substring(0); int i =
	 * rtValue.indexOf("Physical Address. . . . . . . . . :"); while (i > 0) {
	 * rtValue = rtValue.substring(i +
	 * "Physical Address. . . . . . . . . :".length());
	 * address.add(rtValue.substring(0, 18)); i =
	 * rtValue.indexOf("Physical Address. . . . . . . . . :"); } return address;
	 * }
	 */

	// 取得LOCALHOST的IP地址
	/*
	 * private static String getMyIP() { String str = ""; try { str =
	 * InetAddress.getLocalHost().toString(); } catch (UnknownHostException e) {
	 * } return str; }
	 * 
	 * private static String getLocalIP() { String str = getMyIP();
	 * if(str.indexOf("/")>0) { str = str.substring(str.indexOf("/")+1); }
	 * return str; }
	 */

	/**
	 * 将127.0.0.1 形式的IP地址转换成10进制整数
	 * 
	 * @param name
	 * @param @return 设定文件
	 * @return String DOM对象
	 * @Exception 异常对象
	 * @since CodingExample　Ver(编码范例查看) 1.1
	 */
	public static long ipToLong(String strIP) {
		long[] ip = new long[4];
		int position1 = strIP.indexOf(".");
		int position2 = strIP.indexOf(".", position1 + 1);
		int position3 = strIP.indexOf(".", position2 + 1);
		ip[0] = Long.parseLong(strIP.substring(0, position1));
		ip[1] = Long.parseLong(strIP.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(strIP.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(strIP.substring(position3 + 1));
		// ip1*256*256*256+ip2*256*256+ip3*256+ip4
		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}

	/**
	 * 将10进制整数形式转换成127.0.0.1形式的IP地址
	 * 
	 * @param name
	 * @param @return 设定文件
	 * @return String DOM对象
	 * @Exception 异常对象
	 * @since CodingExample　Ver(编码范例查看) 1.1
	 */
	public static String longToIP(long longIP) {
		StringBuffer sb = new StringBuffer("");
		sb.append(String.valueOf(longIP >>> 24));// 直接右移24位
		sb.append(".");
		sb.append(String.valueOf((longIP & 0x00FFFFFF) >>> 16)); // 将高8位置0，然后右移16位
		sb.append(".");
		sb.append(String.valueOf((longIP & 0x0000FFFF) >>> 8));
		sb.append(".");
		sb.append(String.valueOf(longIP & 0x000000FF));
		sb.append(".");
		return sb.toString();
	}

	public static void main(String[] args) {
		/*
		 * List<String> address = IPUtils.getPhysicalAddress(); for(String
		 * add:address) { System.out.printf("物理网卡地址:%s%n", add); } String ip =
		 * IPUtils.getLocalIP(); System.out.println("ip:"+ip); ip="127.0.0.1";
		 * 
		 * System.out.println("bigint:"+IPUtils.stringToBigInt(ip));
		 * System.out.println(IPUtils.getHostName("127.0.0.1")); BigInteger bi =
		 * new BigInteger("2130706433");
		 * System.out.println("str:"+IPUtils.bigIntToString(bi));
		 */
		System.out.println("IP地址的两种表现形式：\r\n");
		// System.out.print("32位形式:");
		// System.out.println(Long.toBinaryString(3396362403L));
		System.out.print("十进制形式:\n");
		System.out.println("202.112.96.163 - " + IPUtils.ipToLong("202.112.96.163"));
		System.out.println("127.0.0.1 - " + IPUtils.ipToLong("127.0.0.1"));
		System.out.print("普通形式:\n");
		System.out.println("3396362403 - " + IPUtils.longToIP(3396362403L));
		System.out.println("0 - " + IPUtils.longToIP(0L));
	}

}
