package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderEventProducer {
    private static final String TOPIC_ADD = "addorder_events";
    private static final String TOPIC_EDIT = "editorder_events";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendAddOrderEvent(Order o){
        kafkaTemplate.send(TOPIC_ADD, JsonUtils.toJson(o));
    }
    public void sendEditOrderEvent(Order o){
        kafkaTemplate.send(TOPIC_EDIT, JsonUtils.toJson(o));
    }

}
