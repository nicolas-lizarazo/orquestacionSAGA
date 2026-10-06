package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    @Autowired
    private OrderService orderService;

    @KafkaListener(topics = "addorder_events", groupId = "order_group")
    public void handleAdd(String json){
        orderService.save(JsonUtils.fromJson(json, Order.class));
    }

    @KafkaListener(topics = "editorder_events", groupId = "order_group")
    public void handleEdit(String json) {
        orderService.save(JsonUtils.fromJson(json, Order.class));
    }
}
