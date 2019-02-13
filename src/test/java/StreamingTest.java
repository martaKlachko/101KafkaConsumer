import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.*;

import java.io.File;

import com.google.common.io.Files;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class StreamingTest {
    private static JavaStreamingContext ssc;
    private static   JavaPairInputDStream<String, String> kafkaSparkPairInputDStream;


    @BeforeClass
    public static void setup() throws IOException {
        ssc = Util.setUpSparkStreamingContext();
        Set<String> topicName = Collections.singleton("test1");
        Map<String, String> kafkaParams = Util.setUpKafka();
        kafkaSparkPairInputDStream = KafkaUtils
                .createDirectStream(ssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicName);
    }



    @After
    public void stopSparkContext() {
        ssc.stop();
    }


    @Test
    public void successfullyCreateSparkStreamWithoutParams()  {
        Assert.assertNotNull("JavaStreamingContext instance should not be null.", ssc);
        Assert.assertNotNull("JavaPairInputDStream instance should not be null.", kafkaSparkPairInputDStream);
        Assert.assertTrue("JavaStreamingContext instance should be of the right type.", ssc instanceof JavaStreamingContext);
        Assert.assertTrue("JavaPairInputDStream instance should be of the right type.", kafkaSparkPairInputDStream instanceof JavaPairInputDStream<?, ?>);
    }
}
