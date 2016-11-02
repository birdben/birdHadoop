### Demo例子是Hadoop自带的WordCount的例子

注意：需要提前安装好Hadoop相关环境

##### 构建birdHadoop.jar

注意：构建birdHadoop.jar之前需要先根据要执行的入口类，修改pom.xml文件中的配置。这样在使用java -jar birdHadoop.jar时候才能找到对应的入口类。

```
$ cd ${workspace}/
$ mvn clean package

# 执行对应入口类的Shell脚本
$ sh runWordCount.sh
```

##### 第一次运行MapReduce程序需要先创建好HDFS目录
```
hdfs dfs -mkdir -p /birdben/input
hdfs dfs -mkdir -p /birdben/output
```