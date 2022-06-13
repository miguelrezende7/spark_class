
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import pandas as ps


#### GRUPO 2 #####

#### Integrantes ########
# Daniela
# Leandro
# Maurício
# Miguel
# Rogério


if __name__ == "__main__":

    spark = SparkSession.builder.appName("Treat JCS AND SCI").getOrCreate()

    jcs = spark.read.load("/home/miguel/impacta-spark/jcs_2020.csv",
                          format="csv", sep=";", inferSchema=True, header=True)

    sci = spark.read.load("/home/miguel/impacta-spark/scimagojr 2020.csv",
                          format="csv", sep=";", inferSchema=True, header=True)
    
    # ETL SCI 
    sci = sci.dropna(subset=['SJR'])
    sci = sci.select("Title", "SJR")
    sci = sci.withColumn('SJR', regexp_replace('SJR', ',', '.'))
    sci = sci.withColumnRenamed("Title", "SCI_Title")
    sci = sci.withColumnRenamed("SJR", "SCI_Factor")
    sci = sci.withColumn("title_lower", sci.SCI_Title)
    sci = sci.withColumn("title_lower", lower(col("title_lower")))

    # ETL JCS 
    jcs = jcs.dropna(subset=['Full Journal Title'])
    jcs = jcs.select("Full Journal Title", "Journal Impact Factor")
    jcs = jcs.withColumnRenamed("Full Journal Title", "JCS_Title")
    jcs = jcs.withColumnRenamed("Journal Impact Factor", "JCS_Factor")
    jcs = jcs.withColumn("title_lower", jcs.JCS_Title)
    jcs = jcs.withColumn("title_lower", lower(col("title_lower")))

    # JOIN SCI AND JCS 
    # df_inner = sci.join(jcs, sci.title_lower == jcs.title_lower, "inner")
    df_outer = sci.join(jcs, sci.title_lower == jcs.title_lower, "outer")
    # df_final = df_inner.union(df_outer)

    df_final=df_outer
    
    # ETL JOINED DF 
    df_final=df_final.fillna({"SCI_Title": "N/A in SCI", "SCI_Factor": 0,"JCS_Title":"N/A in JCS","JCS_Factor":0 })
    df_final = df_final.drop("title_lower")

    
    # EXIBITION COMMANDS 
    df_final.select("*").where((col("SCI_Factor") > 20)).show()
    df_final.select("*").where((col("SCI_Title") == "N/A in SCI")).show()
    df_final.select("*").where((col("JCS_Title") == "N/A in JCS")).show()


    # print(f" \n\n\n {df_final.count()} \n\n\n")

    

    # SAVING COMMANDS 
    # df_final.write.format("json").save(
    #     "/home/miguel/impacta-spark/json_file.json")


    spark.stop()
