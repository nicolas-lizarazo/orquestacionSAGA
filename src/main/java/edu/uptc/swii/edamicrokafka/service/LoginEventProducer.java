package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class LoginEventProducer {
    private static final String TOPIC_ADD = "addlogin_events";
    private static final String TOPIC_EDIT = "editlogin_events";
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendAddLoginEvent(Login l)  {
        kafkaTemplate.send(TOPIC_ADD, JsonUtils.toJson(l));
    }
    public void sendEditLoginEvent(Login l) {
        kafkaTemplate.send(TOPIC_EDIT, JsonUtils.toJson(l));
    }
}
