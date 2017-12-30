# Fel field mapping description

field mapping 工具是根据开源项目fel进行设计实现的。读取mql语句，并编译后等待调用执行。

## mql Examples:

- dateToStr(updateTime,'yyyyMMdd')
- dateToStr(startTime,'yyyyMMdd hhmmss')
- concat(a,b,c)+'abcdefg'
- concat(dateToStr(updateTime, 'yyyyMMdd'), 'asd', c)
- concat(a,'abcdefg',c)
- updateTime
- substr(b)

## mql 语法格式:

- 方法1(参数1, 参数2, 参数3......)
- 参数1,参数2,参数3等可以为常量, 变量名, 方法...

## 支持方法:

- case //尚未实现
- dateToStr(srcField,format) //根据format把srcField从date转成string
- strToDate(srcField,format) //根据format把srcField从string转成date
- concat(srcField1,srcField2,srcField3...)
- substr(srcField,startIndex,endIndex)
