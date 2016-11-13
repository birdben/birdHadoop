#!/bin/bash
local_path=~/Downloads/birdHadoop
local_inputfile_path=$local_path/inputfile/WordCount
local_outputfile_path=$local_path/outputfile/WordCount
if [ -f $local_inputfile_path/input_WordCount.bak ]; then
	# 如果本地bak文件存在，就重命名去掉bak
	echo "正在重命名$local_inputfile_path/input_WordCount.bak文件"
	mv $local_inputfile_path/input_WordCount.bak $local_inputfile_path/input_WordCount
fi
if [ ! -d $local_outputfile_path ]; then
	# 如果本地文件目录不存在，就自动创建
	echo "自动创建$outputfile_path目录"
	mkdir -p $local_outputfile_path
else
	# 如果本地文件已经存在，就删除
	echo "删除$local_outputfile_path/*目录下的所有文件"
	rm -rf $local_outputfile_path/*
fi
# 需要在Maven的pom.xml文件中指定jar的入口类
echo "开始执行birdHadoop.jar..."
storm jar $local_path/target/birdHadoop.jar $local_inputfile_path $local_outputfile_path
echo "结束执行birdHadoop.jar..."
