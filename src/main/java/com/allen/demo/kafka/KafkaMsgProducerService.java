
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
public class KafkaMsgProducerService implements ApplicationRunner {


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
        log.info("upload service producer run,topic:" + topicName);

    }


    public void sendMsgToKafka(JSONObject json) {
        executorGangway.execute(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                boolean result = false;
                try {
                    String jsonResult = JSON.toJSONString(json);
                    long startKafka = System.currentTimeMillis();
                    producer.send(new ProducerRecord<String, String>(topicName, jsonResult));
                    log.info("pluginsUploadService send  data into kafka cost time:" + (System.currentTimeMillis() - startKafka));
                } catch (Exception e) {
                    log.error("pluginsUploadService upload analyze error:" + e.getMessage(), e);
                }
                long end = System.currentTimeMillis();
                log.info("pluginsUploadService analyze cost Time:" + (end - start) + "  result:" + result);

            }
        });
    }

    @Value("${upload.gangway.corePoolSize}")
    public int gangway_corePoolSize;
    @Value("${upload.gangway.maximumPoolSize}")
    public int gangway_maximumPoolSize;
    @Value("${upload.gangway.queueSize}")
    public int gangway_queueSize;

    @Value("${upload.job.corePoolSize}")
    public int job_corePoolSize;
    @Value("${upload.job.maximumPoolSize}")
    public int job_maximumPoolSize;
    @Value("${upload.job.queueSize}")
    public int job_queueSize;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        if (!start) {
            log.info("pluginsUploadService service is not start");
            return;
        }
        kafka();
        executorGangway = new ThreadPoolExecutor(gangway_corePoolSize, gangway_maximumPoolSize,
                5L, TimeUnit.MINUTES,
                new ArrayBlockingQueue(gangway_queueSize));

        log.info("pluginsUploadService service is  start config end");
    }

}
