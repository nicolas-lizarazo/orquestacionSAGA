package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class LoginEventConsumer {
    @Autowired
    private LoginService loginService;

    @KafkaListener(topics = "addlogin_events", groupId = "login_group")
    public void handleAdd(String json) {
        loginService.save(JsonUtils.fromJson(json, Login.class));
    }

    @KafkaListener(topics = "editlogin_events", groupId = "login_group")
    public void handleEdit(String json) {
        loginService.save(JsonUtils.fromJson(json, Login.class));
    }
}