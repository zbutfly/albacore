/**
 * 文件名：ListUtils.java
 * 版本信息：
 * 日期：2010-9-15
 * Copyright 2010 招商银行信息技术部版权所有
 */
package net.butfly.albacore.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.utils.ByteUtils;

/**
 * 项目名称：ebase 名称：ListUtils 描述： 创建人：HO274569 创建时间：2010-9-15 上午10:24:43
 * 
 * @version
 */
public final class ListUtils {

	private ListUtils() {}

	public static List<String> getList(String value, String separator) {
		String[] result = getArray(value, separator);
		return result == null ? null : Arrays.asList(result);
	}

	public static String[] getArray(String value, String separator) {
		if (value == null || value.trim().length() == 0) { return null; }
		return value.split(separator);
	}

	public static List<byte[]> getByteList(List<String> slist) {
		if (slist == null || slist.size() == 0) { return null; }
		List<byte[]> blist = new ArrayList<byte[]>();
		for (String s : slist) {
			blist.add(ByteUtils.hex2byte(s));
		}
		return blist;
	}

	public static List<String> getStringList(List<byte[]> blist) {
		if (blist == null || blist.size() == 0) { return null; }
		List<String> slist = new ArrayList<String>();
		for (byte[] b : blist) {
			slist.add(ByteUtils.byte2hex(b));
		}
		return slist;
	}

	// List<String> -> Strng
	// 分隔符 separator
	// List为null或空, 返回""
	public static String getStringFromStrList(List<String> strList, String separator) {
		if (null == strList) { return ""; }
		StringBuilder sb = new StringBuilder();
		for (String gn : strList) {
			if (null != gn) {
				sb.append(gn).append(separator);
			}
		}
		if (strList.size() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return "";
		}
	}
}
