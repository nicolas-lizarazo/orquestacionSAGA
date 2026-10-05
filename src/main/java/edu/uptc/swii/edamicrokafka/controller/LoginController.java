package edu.uptc.swii.edamicrokafka.controller;

import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.service.LoginEventProducer;
import edu.uptc.swii.edamicrokafka.service.LoginService;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class LoginController {
    @Autowired
    private LoginEventProducer producer;
    @Autowired
    private LoginService loginService;

    @PostMapping("/addlogin")
    public String add(@RequestBody String body) {
        Login loginObj = new Login();
        loginObj = JsonUtils.fromJson(body, Login.class);
        producer.sendAddLoginEvent(loginObj);
        return "Evento enviado";
    }

    @PostMapping("/editlogin")
    public String edit(@RequestBody String body) {
        Login loginObj = new Login();
        loginObj = JsonUtils.fromJson(body, Login.class);
        producer.sendEditLoginEvent(loginObj);
        return "Evento enviado";
    }

    @GetMapping("/findloginbyid/{username}")
    public Login findById(@PathVariable String username) { return loginService.findById(username); }

    @GetMapping("/findalllogins")
    public List<Login> findAll() { return loginService.findAll(); }
}
