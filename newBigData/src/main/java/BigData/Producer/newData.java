package BigData.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

public class newData {
    private  static String topic="dersler";
    public static void main(String[] args) throws FileNotFoundException {
        File dosya=new File("C:\\Users\\HP\\Desktop\\yeni.json");
        Scanner scanner=new Scanner(dosya);
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer producer=new KafkaProducer<String,String>(properties);
        ProducerRecord<String,String > record;
        while(scanner.hasNextLine()){
            String satir=scanner.nextLine();
             record=new ProducerRecord<String, String>(topic,satir);
            producer.send(record); }
       producer.close();

    }
}
