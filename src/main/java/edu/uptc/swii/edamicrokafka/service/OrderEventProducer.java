package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import edu.uptc.swii.edamicrokafka.utils.KafkaEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderEventProducer {

    private static final String TOPIC = "order_events";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Publica un pedido sin clave Kafka. */
    public void sendAddOrderEvent(Order order) {
        sendEventWithoutKey("ADD", JsonUtils.toJson(order));
    }

    /** Publica un pedido utilizando el ID del pedido como clave Kafka. */
    public void sendAddOrderEventWithKey(Order order) {
        KafkaEvent event = new KafkaEvent("ADD", JsonUtils.toJson(order));
        kafkaTemplate.send(TOPIC, order.getId(), JsonUtils.toJson(event));
    }

    public void sendEditOrderEvent(Order order) {
        sendEventWithoutKey("EDIT", JsonUtils.toJson(order));
    }

    public void sendFindOrderByIdEvent(String id) {
        sendEventWithoutKey("FIND_BY_ID", id);
    }

    public void sendFindAllOrdersEvent() {
        sendEventWithoutKey("FIND_ALL", "");
    }

    private void sendEventWithoutKey(String operation, String payload) {
        KafkaEvent event = new KafkaEvent(operation, payload);
        kafkaTemplate.send(TOPIC, JsonUtils.toJson(event));
    }
}