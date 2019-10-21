package com.allen.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * @author bitmain
 * @create: 2019-10-21
 **/
@Component
@Slf4j
public class KafkaMsgConsumer {


    @Value("${upload.kafka.topic}")
    String topicName;

    @KafkaListener(
            topics  = {"${upload.kafka.topic}"},
            groupId = "${upload.kafka.topic}"
    )
    public void structSynData(@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.OFFSET) Long offset,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
                        @Payload String message) {

        log.error("kafka消费者,topic：" + topic + ",消费的消息：" + message);
    }


}
