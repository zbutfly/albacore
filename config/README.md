# albacore-config

> since: albacore-3.0.3-SNAPSHOT

提供基于系统变量、系统属性（Java System Properties）以及配置文件的应用配置载入和管理。

另外，提供了Texts工具和IOs工具。

## Configs

### 配置项

配置项以key=value形式存在，基本类型均为字符串。key为`配置名`，value为`配置值`

### 配置级别

#### 配置源级别

自低而高（当存在相同配置名项，高优先级配置源中定义的配置值将覆盖）依次为：

- 类默认配置文件：以`class-name-default.properties`为文件名的配置文件，搜索路径为：当前`classpath中当前类所在package目录`
- 系统环境变量（System Environment Valiable）：配置名为`下划线分隔的大写字符`
- 指定配置文件：以`class-name.properties`为文件名的配置文件，搜索路径为：当前`运行目录`**或**`classpath`（互斥，找到第一个就不再读取第二个）
- 系统属性（Java System Properties）：配置名为`点分隔的小写字符`，可以通过java命令的-D参数定义

#### 配置目标级别

自低而高（根据配置名自高而低依次读取配制值，读到即返回）依次为：

- MainClass自动绑定
- Class绑定
- 实例绑定
- Field绑定
- 手动读取

## Texts

## IOs