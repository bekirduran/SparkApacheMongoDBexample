package com.bigdata.PoliceCallexample;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().master("local")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/police.callcenter")
                .appName("Police Call Center Service Example").getOrCreate();

        StructType mySchema = new StructType()
                .add("recordId", DataTypes.IntegerType)
                .add("callDateTime", DataTypes.StringType)
                .add("priority", DataTypes.StringType)
                .add("district", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("callNumber", DataTypes.StringType)
                .add("incidentLocation", DataTypes.StringType)
                .add("location", DataTypes.StringType);

        Dataset<Row> rowDataset = sparkSession.read().option("header", true).schema(mySchema).csv("C:\\Users\\morph\\Desktop\\bigdata\\911policeservice.csv");
        Dataset<Row> dataset = rowDataset.filter(rowDataset.col("recordId").isNotNull());
        Dataset<Row> filteredDataSet = dataset.filter(dataset.col("description").notEqual("911/NO  VOICE"));

        Dataset<Row> mostActions = filteredDataSet.groupBy("incidentLocation", "description").count().sort(functions.desc("count"));
        MongoSpark.write(mostActions).mode("overwrite").save();

    }
}
