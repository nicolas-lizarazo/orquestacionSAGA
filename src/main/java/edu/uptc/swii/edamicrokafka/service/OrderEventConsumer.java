package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.saga.SagaMessage;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import edu.uptc.swii.edamicrokafka.utils.KafkaEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderEventConsumer {

    private final OrderService orderService;

    @Value("${server.port:8080}")
    private String port;

    @Value("${app.group:order_group}")
    private String group;

    public OrderEventConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "order_events", groupId = "${app.group:order_group}")
    public void handleOrderEvent(
            String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        System.out.printf("[Kafka] puerto=%s | grupo=%s | particion=%d | clave=%s | evento=%s%n",
                port, group, partition, key, message);

        KafkaEvent event = JsonUtils.fromJson(message, KafkaEvent.class);
        String operation = event.getOperation();
        String payload = event.getPayload();

        switch (operation) {
            case "ADD" -> {
                orderService.save(JsonUtils.fromJson(payload, Order.class));
                System.out.println("Order agregada.");
            }
            case "EDIT" -> {
                orderService.save(JsonUtils.fromJson(payload, Order.class));
                System.out.println("Order editada.");
            }
            case "STATUS_UPDATE" -> {
                SagaMessage msg = JsonUtils.fromJson(payload, SagaMessage.class);
                orderService.updateStatus(msg.getOrderId(), msg.getStatus(), msg.getReason());
                System.out.println("Order " + msg.getOrderId() + " -> " + msg.getStatus());
            }
            case "FIND_BY_ID" -> {
                Order found = orderService.findById(payload);
                System.out.println("Order encontrada: " + found);
            }
            case "FIND_ALL" -> {
                List<Order> all = orderService.findAll();
                System.out.println("Total órdenes: " + all.size());
            }
            default -> System.out.println("Operación no soportada en order_events: " + operation);
        }
    }
}