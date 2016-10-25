#!/bin/bash
workspace_path=/Users/yunyu/workspace_git/birdHadoop
# 需要将我们要分析的track.log日志文件上传到HDFS文件目录下
hdfs dfs -put ${workspace_path}/inputfile/WordCount/input_WordCount /birdben/input
# 需要先删除HDFS上已存在的目录，否则hadoop执行jar的时候会报错
hdfs dfs -rm -r /birdben/output
# 需要在Maven的pom.xml文件中指定jar的入口类
hadoop jar ${workspace_path}/target/birdHadoop.jar /birdben/input /birdben/output
# 如果在Maven的pom.xml文件中没有指定jar的入口类，则需要在hadoop执行jar的时候指定入口类
# hadoop jar ${workspace_path}/target/birdHadoop.jar com.birdben.mapreduce.demo.WordCount /birdben/input /birdben/output
# 从HDFS目录中导出mapreduce的结果文件到本地文件系统 
hdfs dfs -get /birdben/output/* ${workspace_path}/outputfile/WordCount/
