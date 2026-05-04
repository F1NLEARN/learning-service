package com.finlearn.learningservice.infrastructure.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class UserEventConsumer {

    @KafkaListener(
            topics = "${kafka.topics.user.registered:finlearn-user-registered}",
            groupId = "${spring.kafka.consumer.group-id:learning-service}"
    )
    public void handleUserRegistered(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack
    ) {
        try {
            log.info("[Kafka] UserRegistered 이벤트 수신 - topic={}, partition={}, offset={}, payload={}",
                    topic, partition, offset, payload);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Kafka] UserRegistered 이벤트 처리 실패 - topic={}, offset={}, error={}",
                    topic, offset, e.getMessage());
        }
    }
}
