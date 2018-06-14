package com.example.k;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;


@SpringBootApplication
public class KApplication {

    public static void main(String[] args) {
        SpringApplication.run(KApplication.class, args);
    }

    @KafkaListener(topics = "test")
    public void listenT2(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.printf("topic==%s,key==%s,value==%s%n", cr.topic(),cr.key().toString(), cr.value().toString());
        String mysql = Maxwell2MysqlUtil.maxwell2Mysql(cr);
        System.out.printf("mysql===%s%n", mysql);
    }
}
