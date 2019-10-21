
package com.allen.demo.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author bitmain
 */
@ConditionalOnProperty(name = "upload.start", havingValue = "true")
@Component
@Slf4j
public class KafkaMsgProducer implements ApplicationRunner {


    static ThreadPoolExecutor executorGangway = null;


    @Value("${upload.start}")
    boolean start;

    Producer<String, String> producer;

    @Value("${upload.kafka.topic}")
    String topicName;

    @Value("${upload.kafka.servers}")
    String kafkaServers;


    private void kafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("acks", "0");
        props.put("retries", 0);
        //160k
        props.put("batch.size", 16384 * 20);
        props.put("linger.ms", 400);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        log.info("初始化kafka完成,topic:" + topicName);

    }


    public void sendMsgToKafka(JSONObject json) {
        executorGangway.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    String jsonResult = JSON.toJSONString(json);
                    long startKafka = System.currentTimeMillis();
                    producer.send(new ProducerRecord<String, String>(topicName, "myApp",jsonResult));
                    log.info("发送消息到topic:" + topicName + ",耗时：" + (System.currentTimeMillis() - startKafka));
                } catch (Exception e) {
                    log.error("发送消息到topic:" + topicName + ",发送错误，error:" + e.getMessage(), e);
                }

            }
        });
    }

    @Value("${upload.gangway.corePoolSize}")
    public int gangway_corePoolSize;
    @Value("${upload.gangway.maximumPoolSize}")
    public int gangway_maximumPoolSize;
    @Value("${upload.gangway.queueSize}")
    public int gangway_queueSize;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        if (!start) {
            log.info("kafka,topic" + topicName + ",未设置初始化启动>>>>>>>>>>");
            return;
        }
        kafka();
        executorGangway = new ThreadPoolExecutor(gangway_corePoolSize, gangway_maximumPoolSize,
                5L, TimeUnit.MINUTES,
                new ArrayBlockingQueue(gangway_queueSize));

        log.info("kafka,topic" + topicName + ",初始化启动配置完成");
    }

}
