package edu.uptc.swii.edamicrokafka.controller;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.service.OrderEventProducer;
import edu.uptc.swii.edamicrokafka.service.OrderService;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class OrderController {

    @Autowired
    private OrderEventProducer producer;

    @Autowired
    private OrderService orderService;

    @PostMapping("/addorder")
    public String addOrder(@RequestBody String body){
        producer.sendAddOrderEvent(JsonUtils.fromJson(body, Order.class));
        return "Evento enviado";
    }

    @PostMapping("/editorder")
    public String editOrder(@RequestBody String body){
        producer.sendEditOrderEvent(JsonUtils.fromJson(body, Order.class));
        return "Evento enviado";
    }

    @GetMapping("/findorderbyid/{id}")
    public Order findById(@PathVariable String id){
        return orderService.findById(id);
    }

    @GetMapping("/findallorders")
    public List<Order> findAllOrders(){
        return orderService.findAll();
    }



}
