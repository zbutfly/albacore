Fel field mapping description
=====

field mapping 工具是根据开源项目fel进行设计实现的。读取mql语句，并编译后等待调用执行。

mql Examples:
-----
	update=dateToString(updateTime,'yyyyMMdd')?updateTime:DATE#dateToString
	start=dateToString(startTime,'yyyyMMdd hhmmss')?startTime:DATE#dateToString
	concatStr=concat(a,b,c)+'abcdefg'?a:STRING&b:STRING&c:STRING#concat
	concatTest=concat(dateToString(updateTime, 'yyyyMMdd'), 'asd', c)?updateTime:DATE&c:STRING#dateToString&concat  //方法嵌套
	concatTest1=concat(a,'abcdefg',c)?a:STRING&c:STRING#concat
	start1=rename(startTime)?startTime:DATE#rename
	update1=updateTime?updateTime:DATE
	//numLong=rename(numberLong)?numberLong:LONG#rename (//为注释标识)
	//changed=rename(a)?a:STRING#rename
	//subStringed=substr(b)?b:STRING#substr

mql 语法格式:
-----
	{目标字段}=方法1(参数1, 参数2, 参数3......)?源字段1:源字段类型1&源字段2:源字段类型2...#方法1&方法2...
	参数1,参数2,参数3等可以为常量, 变量名, 方法...

支持接收dpc配置, 用法如下:
-----
	1.
	MQLParser.buildMQLEntity(String dstField, String expressionStr, Map<String, String> srcField, String[] functions)
	返回一个MQLEntity
	dstField是目标字段的字段名
	expressionStr是表达式字符串
	srcField是源字段的字段名和字段类型的键值对
	functions是表达式字符串中出现的方法名，支持方法在下面
	2.
	FelBuilder.mapping(originMap, dstMap, entity)可以实现从originMap到dstMap
	originMap为源数据键值对<String, Object>
	dstMap为目标数据键值对<String, Object>
	entity为步骤1 创建的MQLEntity

例子:
-----
	public void testEntityParser() {
		Map<String, Object> originMap = new HashMap<>();
		Map<String, Object> dstMap = new HashMap<>();
		originMap.put("startTime", new Date(1334007167000L));
		originMap.put("updateTime", new Date(631163535000L));
		originMap.put("a", "hello");
		originMap.put("b", "world");
		originMap.put("c", "!");
		Map<String, String> srcFields = new HashMap<>();
		srcFields.put("updateTime", "DATE");
		srcFields.put("c", "STRING");
		MQLEntity entity = MQLParser.buildMQLEntity("concatTest",
				"concat(dateToString(updateTime, 'yyyyMMdd'), 'asd', c)", srcFields,
				new String[] { "dateToString", "concat" });
		FelBuilder.mapping(originMap, dstMap, entity);
		System.out.println(dstMap.toString());
	}

支持方法:
-----
	dateToString(srcField,format) //根据format把srcField从date转成string
	stringToDate(srcField,format) //根据format把srcField从string转成date
	concat(srcField1,srcField2,srcField3...)
	substr(srcField,startIndex,endIndex)
	rename(srcField)