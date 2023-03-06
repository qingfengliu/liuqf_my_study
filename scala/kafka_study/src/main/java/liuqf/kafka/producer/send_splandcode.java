package liuqf.kafka.producer;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;



public class send_splandcode {
    public static void main(String[] args) throws Exception {
        //0配置 连接集群
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"liuqf1:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //1 创建生产者对象
        // "" hello
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<>(properties);
        //读取csv文件 打印文件中每一行

        try{
            CSVReader reader = new CSVReader(new FileReader("D:/chanpin.csv"));

            List<String[]> list = reader.readAll();
            for (String[] item : list) {
                List tmp=Arrays.asList(item);
                kafkaProducer.send(new ProducerRecord<>("first",StringUtils.join(tmp,",")));
            }
        } catch(Exception e) {
            System.out.println(e);
            throw new Exception();
        }


    }
}
