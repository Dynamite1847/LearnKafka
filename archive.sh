#！/bin/bash

#判断输入参数个数是否为1
if [ $# -ne 1 ]
then
	echo "参数个数错误！应输入一个参数，作为归档目录名"
	exit
fi

#从参数中获取目录
if [ -d $1 ]
then
  echo
  echo "$1目录存在"
else
  echo
  echo "$1目录不存在"
  echo
  exit
fi

DIR_NAME=$(basename $1)
DIR_PATH=$(cd $(dirname $1); pwd)

# 获取当前日期
DATE=$(date +%y%m%d)

#定义归档文件名称
FILE=archive_${DIR_NAME}_$DATE.tar.gz
DESTINATION=/root/archive/$FILE

#开始归档目录文件
echo "开始归档${DESTINATION}"
echo

tar -czf