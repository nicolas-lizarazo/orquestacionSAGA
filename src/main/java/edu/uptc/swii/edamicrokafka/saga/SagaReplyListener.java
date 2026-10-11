package edu.uptc.swii.edamicrokafka.saga;

import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import edu.uptc.swii.edamicrokafka.utils.KafkaEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SagaReplyListener {

    private final ReplyRegistry registry;

    public SagaReplyListener(ReplyRegistry registry) {
        this.registry = registry;
    }

    @KafkaListener(topics = SagaTopics.VALIDATE_CUSTOMER_REPLY, groupId = "saga_orchestrator_group")
    public void onCustomerReply(String message) {
        handle(SagaTopics.VALIDATE_CUSTOMER_REPLY, message);
    }

    @KafkaListener(topics = SagaTopics.PAYMENT_REPLY, groupId = "saga_orchestrator_group")
    public void onPaymentReply(String message) {
        handle(SagaTopics.PAYMENT_REPLY, message);
    }

    private void handle(String topic, String message) {
        KafkaEvent event = JsonUtils.fromJson(message, KafkaEvent.class);
        SagaMessage reply = JsonUtils.fromJson(event.getPayload(), SagaMessage.class);
        System.out.printf("[Saga] <- %s | %s %s%n", topic, event.getOperation(), event.getPayload());
        registry.complete(topic, reply);
    }
}