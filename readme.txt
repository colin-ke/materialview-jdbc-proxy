    如果从数据库中做一些表关联或汇总逻辑时，有些数据量大，查询慢，为了提高用户体验，
一般是把数据预先处理好，将结果存储到物理表中，报表从这预先处理后的数据查询，但是
如此增加了报表的开发和维护的成本，如果能够有一个功能：查询还是从原始表查询数据，
但是当发现性能比较差的时候，能够像建立索引的方式快速，透明优化性能，将是非常棒的
事情，这个思路就是物化视图的思路，也是本项目做的事情。
    
   本项目工作在JDBC驱动层，按照JDBC规范做了一个驱动代理，应用层访问我们的驱动，
我们识别和分析sql，将sql改写成从物化视图中去查询，从而快速提高查询性能。
集成时，只需替换到JDBC配置参数，即可快速被集成到现有项目中，做到最低的侵入性。

   如物化视图的创建语句：
   create materialized view mv_sales_fact as 
	SELECT time_id,product_id,
	COUNT(time_id) c ,
	count(distinct time_id)dc,
	count(distinct customer_id)dcd,
	sum(time_id)sc
    FROM sales_fact_1997  
    GROUP BY time_id,product_id
    当创建物化视图时，会把视图对应的sql执行一遍，把执行的结果存储到真实的物理表
中，所以下面的sql会被分析出能够从物化视图查询，从而转到物化视图查询
应用程序SQL：
SELECT time_id,product_id,COUNT(time_id) c 
FROM sales_fact_1997 WHERE product_id>1537 
GROUP BY time_id,product_id ;
被重写之后的SQL：
SELECT mv_sales_fact.time_id AS time_id,mv_sales_fact.product_id AS product_id,mv_sales_fact.c AS c
FROM mv_sales_fact
WHERE mv_sales_fact.product_id > 1537

