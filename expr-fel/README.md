# EXPR-FEL表达式引擎

根据开源项目FEL进行设计实现的。读取MQL语句，预编译（precompile）后调用执行。


## MQL语法格式

方法1(参数1, 参数2, 参数3......)

> 参数1,参数2,参数3等可以为常量, 变量名, 方法...

样例：

- dateToStr(updateTime,'yyyyMMdd')
- dateToStr(startTime,'yyyyMMdd hhmmss')
- updateTime
- substr(b)

## 支持方法

### 系统函数

#### isNull

> 判断是否为null

- 调用参数：isNull(obj)
- 返回类型：Boolean

#### equal

> 判断对象是否equal

- 调用参数：equal(obj, obj)
- 返回类型：Boolean

### 日期函数

#### dateToStr

> 日期格式化，格式字符串参考：[JDK文档](https://docs.oracle.com/javase/9/docs/api/java/text/SimpleDateFormat.html)

- 调用参数：dateToStr(date, format)
- 返回类型：String

#### strToDate

> 字符串转日期，格式字符串参考：[JDK文档](https://docs.oracle.com/javase/9/docs/api/java/text/SimpleDateFormat.html)

- 调用参数：strToDate(date, format)
- 返回类型：Date

#### millsToDate

> 毫秒转日期，可以带时区。毫秒数接受字符串类型或数字类型长整数，时区为带符号的整数，例如：北京时区为+8

- 调用参数：strToDate(ms<, timezone>)
- 返回类型：Date

### 字符串函数

#### ~~concat~~

> 字符串连接，==**已经废弃**==，使用操作符+

- 调用参数：~~concat(str1, str2, str3, …)~~
- 返回类型：String

#### substr

> 字符串截取，参考：[JDK文档](https://docs.oracle.com/javase/9/docs/api/java/lang/String.html#substring-int-int-)

- 调用参数：substr(str, begin, end)
- 返回类型：String

#### strlen

> 字符串长度

- 调用参数：strlen(str)
- 返回类型：Integer

#### uuid

> 生成随机UUID字符串（Java规范）

- 调用参数：uuid()
- 返回类型：Object

#### strrev

> 倒转字符串

- 调用参数：strrev('1234567')
- 返回类型：String

#### case

> 条件判断和转移

- 调用参数：case(value, case1, result1, case2, result2, ... [default])
- 返回类型：Object

#### match

> 正则表达式匹配

- 调用参数：match(value, regularExpression)
- 返回类型：Boolean

#### strpadl

> 左填充字符c直到结果字符串长度为l

- 调用参数：strpadl(str, len, char)
- 返回类型：String

#### strpadr

> 右填充字符c直到结果字符串长度为l

- 调用参数：strpadr(str, len, char)
- 返回类型：String

#### strfil

> 重复字符n次创建字符串

- 调用参数：strfil(n, char)
- 返回类型：String

#### trim

> 去除前后空格

- 调用参数：trim(str)
- 返回类型：String

#### replace

> 替换字符串中的特定字符

- 调用参数：replace(inputString, oldcharArr, newcharArr)
- 返回类型：String

#### triml

> 去除字符串前面部份空格或者tab

- 调用参数：triml(str)
- 返回类型：String

#### trimr

> 去除字符串后面部份空格或者tab

- 调用参数：trimr(str)
- 返回类型：String

#### upper

> 字符串转大写

- 调用参数：upper(str)
- 返回类型：String

#### lower

> 字符串转小写

- 调用参数：lower(str)
- 返回类型：String

#### split

> 字符串切分

- 调用参数：split(str, splitter)
- 返回类型：List&lt;String&gt;