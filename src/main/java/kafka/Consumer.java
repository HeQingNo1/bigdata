package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // 定义kakfa 服务的地址，不需要将所有broker指定上
        properties.put("bootstrap.servers","192.168.81.101:9092");
        // 制定consumer group
        properties.put("group.id","g1");
        // 是否自动确认offset
        properties.put("enable.auto.commit","true");
        // 自动确认offset的时间间隔
        properties.put("auto.commit.interval.ms","1000");
        // key的序列化类
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        // 定义consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer(properties);
        // 消费者订阅的topic
        consumer.subscribe(Collections.singleton("test1"));
        // 消费者订阅的topic, 可同时订阅多个
        //consumer.subscribe(Arrays.asList("first", "second","third"));

        while (true){
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String,String> records = consumer.poll(100);
            System.out.println(1);
            for (ConsumerRecord<String,String> record :records){
                System.out.println(record.offset() + "  " + record.key()+":"+record.value());
            }
        }



    }
}
