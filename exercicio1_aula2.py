import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf


#### GRUPO 2 #####

#### Integrantes ########
# Daniela
# Leandro
# Maurício
# Miguel
# Rogério



if __name__ == "__main__":

    def print_values(x):
        print(f'{x[0]}: {x[1]}')
        

    
    spark = SparkSession.builder.appName("Count Words").getOrCreate()
    sc=spark.sparkContext
    path = "/home/miguel/impacta-spark/lorem_ipsum.txt"
    df1=sc.textFile(path)
    # print(df1.collect())
 
    df1=df1.map(lambda x:str(x).replace(".",""))
    df1=df1.map(lambda x:str(x).replace(",",""))
    df1=df1.map(lambda x:str(x).replace(";",""))
    df1=df1.map(lambda x:str(x).lower())
    df2=df1.flatMap(lambda x: x.split(" "))

    # df3=df2.map(print_values).collect()

    df3=df2.map(lambda x:((x,1)))
    # print(df5.collect())

    df4=df3.reduceByKey(lambda x,y : x+y)
    # print(df6.collect())

    df4=df4.filter(lambda x:x[0]!="")

    df5=df4.sortBy(lambda x:x[1])
    # print(df5.collect())
    
    # df5.map(print_values).collect()

    df5.foreach(print_values)
  
    spark.stop()