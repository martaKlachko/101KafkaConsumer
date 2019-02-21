import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;


public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        //configuring environment
        JavaStreamingContext ssc = Util.setUpSparkStreamingContext();
        Set<String> topicName = Collections.singleton("test");
        Map<String, String> kafkaParams = Util.setUpKafka();

        //reading kafka topic
        JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
                .createDirectStream(ssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicName);
        //obtaining messages
        JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream
                .map((Function<Tuple2<String, String>, String>) tuple2 -> (tuple2._2()));

        kafkaSparkInputDStream.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
            JavaPairRDD<Row, Date> rowRDD = rdd.mapToPair((PairFunction<String, Row, Date>) msg -> {

                Row row = RowFactory.create(msg);
                return new Tuple2<>(row, Date.from(Instant.now()));
            });
            //Create Schema
            StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("Message", DataTypes.StringType, true),
                    DataTypes.createStructField("Time", DataTypes.DateType, true)});
            //Get Spark 2.0 session
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
            JavaRDD<Row> row1RDD = rowRDD.map(tuple -> RowFactory.create(tuple._1(), tuple._2()));

            Dataset<Row> msgDataFrame = spark.createDataFrame(row1RDD, schema);
            msgDataFrame.show();
            JavaEsSparkSQL.saveToEs(msgDataFrame, "myindex/docum");
        });


//
        ssc.start();
        ssc.awaitTermination();
    }
}