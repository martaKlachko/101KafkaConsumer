
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Util {
    public static Configuration setUpConf() {

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        return conf;

    }

    public static JavaStreamingContext setUpSparkStreamingContext() {

        SparkConf sparkConf = new SparkConf().setAppName("kafkaSparkStream")
                .setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));


        return ssc;

    }

    public static Map<String, String> setUpKafka() {
        Map<String, String> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        kafkaParams.put("group.id", "1");
        return kafkaParams;
    }


}
