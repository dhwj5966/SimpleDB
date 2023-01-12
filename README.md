# SimpleDataBase

SDB 是一个 Java 实现的简单的数据库，部分原理参照自 MySQL。实现了以下功能：

- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁检测与处理
- 简单的表和字段管理
- 简陋的 SQL 解析
- 基于 socket 的 server 和 client

## 快速启动
- 启动环境：需要JDK (11及以上版本) 和Maven

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先切换到POM文件所在的文件夹，执行以下命令编译源码：

```shell
mvn compile
```

1.创建数据库。执行以下命令以 C:\Users\windows\Desktop 作为路径创建名为test的数据库：

```shell
mvn exec:java -Dexec.mainClass="top.wuzonghui.simpledb.backend.Launcher" -Dexec.args="-create C:\Users\windows\Desktop\test"
```

2.开启数据库服务。通过以下命令以默认参数启动指定路径的数据库服务，在启动数据库服务前需要先创建数据库：

```shell
mvn exec:java -Dexec.mainClass="top.wuzonghui.simpledb.backend.Launcher" -Dexec.args="-open C:\Users\windows\Desktop\test"
```

3.这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库，该命令会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。

```shell
mvn exec:java -Dexec.mainClass="top.wuzonghui.simpledb.client.Launcher"
```

# SDB支持的SQL语法
- \<begin statement>

    begin 开启一个隔离级别为ReadCommitted的事务
    
    begin isolation level repeatable read 开启一个隔离级别为RepeatableRead的事务


- \<commit statement>

    commit 


- \<create statement>

  create table [tablename] [fieldname] [fieldtype] [fieldname] [fieldtype] ... [fieldname] [fieldtype][(index [fieldnamelist])]
  
  示例： create table users id int32,name string(index id name)
  
  支持的fieldtype:int32,int64,string


- \<select statement>

  select (*|[field name list]) from [table name] [\<where statement>]

  select * from student where id = 1

  select name from student where id > 1 and id < 4

  select name, age, id from student where id = 12


- \<insert statement>

  insert into [table name] values [value list]

  insert into student values 5 "Zhang Yuanjia" 22


- \<delete statement>

  delete from [table name] [where statement]

  delete from student where name = "Zhang Yuanjia"


- \<update statement>

  update [table name] set [field name] = [value] [\<where statement>]
  
  update student set name = "ZYJ" where id = 5


- \<others>

  quit、exit 退出客户端

  show tables

# 执行示例：

![](https://s3.bmp.ovh/imgs/2023/01/12/0b4a8e56fa9574f1.png)





