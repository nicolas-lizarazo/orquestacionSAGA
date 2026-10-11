package edu.uptc.swii.edamicrokafka.saga;

import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import edu.uptc.swii.edamicrokafka.utils.KafkaEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SagaPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public SagaPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, String operation, SagaMessage message) {
        KafkaEvent event = new KafkaEvent(operation, JsonUtils.toJson(message));
        kafkaTemplate.send(topic, message.getOrderId(), JsonUtils.toJson(event));
        System.out.printf("[Saga] -> %s | clave=%s | %s %s%n",
                topic, message.getOrderId(), operation, event.getPayload());
    }
}