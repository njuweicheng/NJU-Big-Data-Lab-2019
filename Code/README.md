# 程序运行说明 #

## Hadoop ##
### 运行环境 ###
- Java版本： 1.7
- Hadoop版本： 2.7.1
- 系统环境： Linux Ubuntu
- Ide： Intellij IDEA

### 代码结构 ###
finalLab.zip解压缩即为该Maven项目：

1. pom.xml

	Maven项目的项目对象模型，添加了依赖包及jar包的打包方式
2. src/main/java
	- task1
	- task2
	- task3
	- task4
	- task5\_dks：邓开圣实现的异步标签传播算法
	- task5\_qy：戚赟实现的同步标签传播算法
	- task6\_dks：邓开圣对应的task6实现
	- task6\_qy：戚赟对应的task6实现
	- MainDriver.java： 调用上述package，作为程序的main class

### 编译打包 ###
使用Intellij IDEA创建Maven项目，在IDEA中点击打包，方式如下：

`maven clean`

`maven package`

得到包含依赖的jar包：`target/finalLab-1.0-SNAPSHOT-jar-with-dependencies.jar`

### jar包运行方式 ###
本项目将所有task实现组合在一起，用以下命令执行jar包：


    hadoop jar finalLab-1.0-SNAPSHOT-jar-with-dependencies.jar /data/task2/people_name_list.txt /data/task2/novels task1_output task2_output task3_output task4_output1 task4_output2 task4_output task5_dks_output task6_dks_output task5_qy_output task6_qy_output

上述output目录运行前均需不存在，每个task的输出分别存储在以下目录：

- task1\_output
- task2\_output
- task3\_output
- task4\_output
- task5\_dks\_output/data10、task5\_qy\_output/15
- task6\_dks\_output、task6\_qy\_output


## Spark ##
### 运行环境 ###
- Spark版本： 2.4.3
- Spark安装模式：Standalone
- Scala版本： 2.12.6

### 代码结构 ###
sparkFinalLab.zip解压缩即为该sbt项目：

1. build.sbt

	sbt项目用于添加依赖包的配置文件

2. src/main/scala/FinalLab
	- spark_task1.scala
	- spark_task2.scala
	- spark_task3.scala
	- spark_task4.scala
	- spark_task5.scala
	- spark_task6.scala

### 运行方式 ###
Spark部分未打包，在IDEA项目中按顺序运行上述scala代码即可。

\* 输入文件（人名列表和小说集）已置于项目文件中，请勿更改其位置

输出结果即为：

- task1\_output
- task2\_output
- task3\_output
- task4\_output
- task5\_output
- task6\_output