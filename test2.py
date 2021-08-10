from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)




sqlContext.sql("insert overwrite table sputnik.finalraw select tested_date, state, sum(case when status='Confirmed' then num_cases end) as confirmed, sum(case when status ='Recovered' then num_cases end) as recovered, sum(case when status ='Deceased' then num_cases end) as deceased from sputnik.rawdata group by tested_date,state order by unix_timestamp(tested_date,'dd-MM-yyyy')");


sqlContext.sql("insert overwrite table sputnik.previous select tested_date,state,total_tested, lag(total_tested,1,0)  over(partition by state order by unix_timestamp(tested_date, 'dd-MM-yyyy')) as previous_day_cumulative_tested from sputnik.state_data2");


sqlContext.sql("insert overwrite table sputnik.each_day select tested_date, state,  total_tested-previous_day_cumulative_tested as total_tested from sputnik.previous");


sqlContext.sql("insert overwrite table sputnik.result select e.tested_date, e.state, r.confirmed, r.recovered, r.deceased, e.total_tested from sputnik.each_day e inner join sputnik.finalraw r on e.tested_date=r.tested_date and e.state=r.state order by unix_timestamp(e.tested_date,'dd-MM-yyyy'),e.state");
