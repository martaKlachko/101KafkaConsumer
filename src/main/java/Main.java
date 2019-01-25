import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/spark/mystream/"), conf);


        SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        Map<String, String> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("group.id", "1");
        Set<String> topicName = Collections.singleton("test1");

        JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
                .createDirectStream(ssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicName);

        JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream
                .map((Function<Tuple2<String, String>, String>) tuple2 -> tuple2._2());

        kafkaSparkInputDStream.dstream().saveAsTextFiles("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/spark/mystream/","txt");
        ssc.start();
        ssc.awaitTermination();
    }
}