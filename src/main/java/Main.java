import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        Configuration conf = Util.setUpConf();
        FileSystem fs = FileSystem.get(URI.create("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/spark/mystream/"), conf);


        JavaStreamingContext ssc = Util.setUpSparkStreamingContext();
        Set<String> topicName = Collections.singleton("test1");
        Map<String, String> kafkaParams = Util.setUpKafka();
        JavaPairInputDStream<String, String> kafkaSparkPairInputDStream = KafkaUtils
                .createDirectStream(ssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicName);

        JavaDStream<String> kafkaSparkInputDStream = kafkaSparkPairInputDStream
                .map((Function<Tuple2<String, String>, String>) tuple2 -> tuple2._2());

        kafkaSparkInputDStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                rdd.coalesce(1).saveAsTextFile("hdfs://sandbox-hdp.hortonworks.com:8020/user/hadoop/spark/mystream");
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}