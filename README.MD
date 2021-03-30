### hive 表工具
1. 合并hive表小文件

   原理：扫描hive库中表的hdfs目录，如果总文件数少于* -fs *指定的文件个数或者大于（Math.max(1,totalFile Size/hdfsBlockSize）数，则通过spark thrift server提供的hint, *COALESCE* 实现对小文件的合并

   

