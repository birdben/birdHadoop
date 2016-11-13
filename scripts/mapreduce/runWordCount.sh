#!/bin/bash
local_path=~/Downloads/birdHadoop
hdfs_input_path=/birdben/input
hdfs_output_path=/birdben/output
# 在HDFS上创建需要分析的文件存储目录，如果已经存在就先删除再重新创建，保证脚本的正常执行
echo "删除HDFS上的input目录$hdfs_input_path"
hdfs dfs -rm -r $hdfs_input_path
echo "创建HDFS上的input目录$hdfs_input_path"
hdfs dfs -mkdir -p $hdfs_input_path
# 需要将我们要分析的track.log日志文件上传到HDFS文件目录下
echo "将$local_path/inputfile/WordCount/input_WordCount文件复制到HDFS的目录$hdfs_input_path"
hdfs dfs -put $local_path/inputfile/WordCount/input_WordCount $hdfs_input_path
# 需要先删除HDFS上已存在的目录，否则hadoop执行jar的时候会报错
echo "删除HDFS的output目录$hdfs_output_path"
hdfs dfs -rm -r -f $hdfs_output_path
# 需要在Maven的pom.xml文件中指定jar的入口类
echo "开始执行birdHadoop.jar..."
hadoop jar $local_path/target/birdHadoop.jar $hdfs_input_path $hdfs_output_path
echo "结束执行birdHadoop.jar..."

if [ ! -d $local_path/outputfile/WordCount ]; then
	# 如果本地文件目录不存在，就自动创建
	echo "自动创建$local_path/outputfile/WordCount目录"
	mkdir -p $local_path/outputfile/WordCount
else
	# 如果本地文件已经存在，就删除
	echo "删除$local_path/outputfile/WordCount/*目录下的所有文件"
	rm -rf $local_path/outputfile/WordCount/*
fi
# 从HDFS目录中导出mapreduce的结果文件到本地文件系统
echo "导出HDFS目录$hdfs_output_path目录下的文件到本地$local_path/outputfile/WordCount/"
hdfs dfs -get $hdfs_output_path/* $local_path/outputfile/WordCount/
