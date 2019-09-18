# labelResultDataStatics
标签结果数据分析

此脚本当前为分步执行，执行过程如下：
1、配置输出表文件
2、执行脚本，得到获取表字段 get_column.sql
3、执行获取表字段 get_column.sql文件，得到表字段结果
4、解析表字段结果，构建获取表字段极值、null值的 get_extreme.sql
5、执行 get_extreme.sql 文件，得到字段极值、null值的结果
6、解析极值结果文件，转化成excel格式输出excel文件。
