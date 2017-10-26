package BigData.Consumer;

import com.mongodb.spark.MongoSpark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

import java.util.*;

public class ProcessedDatas {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf= new SparkConf()
                .setAppName("Veriisleme")
                .setMaster("local");
        JavaStreamingContext streamingContext=
                new JavaStreamingContext(sparkConf,new Duration(5000));
        final SparkSession spark = SparkSession
                .builder()
                .config("spark.mongodb.output.uri", "mongodb://localhost/test.myCollection")
                .getOrCreate();
        Map<String, Object> kafkaParams =
                new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "ders");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics= Arrays.asList("dersler");

        JavaInputDStream<ConsumerRecord<String,String>>stream=
                KafkaUtils
                        .createDirectStream(streamingContext,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String,String >Subscribe(topics,kafkaParams));

        JavaDStream<String> gelenVeri =
                stream.map(new Function<ConsumerRecord<String, String>, String>() {
                    public String call(ConsumerRecord<String, String> icerik) throws Exception {
                        return icerik.value();
                    }
                });
//gelenVeri.print();
        JavaEsSparkStreaming.saveJsonToEs(gelenVeri,"derslerim/bilgiler");
    gelenVeri.foreachRDD(new VoidFunction<JavaRDD<String>>() {
         public void call(JavaRDD<String> stringJavaRDD) throws Exception {
             Dataset<Row> data=spark.read().json(stringJavaRDD.rdd());
             data.show();
             data.createOrReplaceTempView("dersicerigi");
             Dataset<Row> newData=spark.sql("Select * from dersicerigi");
             MongoSpark.write(newData).option("collection","derslerim").mode("overwrite").save();
         }
     });

        streamingContext.start();
        streamingContext.awaitTermination();


    }
}
