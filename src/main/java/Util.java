
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;

public class Util {
    //configuring hdfs
    public static Configuration setUpConf() {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        return conf;

    }

    //configuring spark streaming context
    public static JavaStreamingContext setUpSparkStreamingContext() {

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkWithElastic")
                .setMaster("local[*]")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "localhost")
                .set("es.port", "9200")
                .set("es.http.timeout", "3m")
                .set("es.scroll.size", "50")
                .set("es.spark.sql.streaming.sink.log.enabled", "false").
                set("spark.sql.streaming.checkpointLocation", "/spark/docs");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));


        return ssc;

    }

    //configuring kafka
    public static Map<String, String> setUpKafka() {
        Map<String, String> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("message.timestamp.type", "LogAppendTime");
        kafkaParams.put("includeTimestamp", "true");

        return kafkaParams;
    }


}
