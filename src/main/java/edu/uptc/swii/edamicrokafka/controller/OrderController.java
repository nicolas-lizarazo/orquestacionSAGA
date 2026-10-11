package edu.uptc.swii.edamicrokafka.controller;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.saga.OrderSagaWorkflow;
import edu.uptc.swii.edamicrokafka.service.OrderEventProducer;
import edu.uptc.swii.edamicrokafka.service.OrderService;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
public class OrderController {

    @Autowired
    private OrderEventProducer producer;

    @Autowired
    private OrderService orderService;

    @Autowired
    private WorkflowClient workflowClient;

    @Value("${temporal.task-queue:ORDER_SAGA_QUEUE}")
    private String taskQueue;

    @PostMapping("/addorder")
    public String addOrder(
            @RequestBody String body,
            @RequestParam(defaultValue = "false") boolean withKey) {
        Order order = JsonUtils.fromJson(body, Order.class);
        if (withKey) {
            producer.sendAddOrderEventWithKey(order);
        } else {
            producer.sendAddOrderEvent(order);
        }
        return withKey
                ? "Evento enviado con clave Kafka = " + order.getId()
                : "Evento enviado sin clave Kafka";
    }

    @PostMapping("/saga/addorder")
    public Map<String, String> addOrderSaga(@RequestBody String body) {
        Order order = JsonUtils.fromJson(body, Order.class);
        if (order.getId() == null || order.getId().isBlank()) {
            order.setId(UUID.randomUUID().toString());
        }
        order.setStatus("PENDING");

        String workflowId = "order-saga-" + order.getId();
        OrderSagaWorkflow workflow = workflowClient.newWorkflowStub(
                OrderSagaWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(taskQueue)
                        .setWorkflowId(workflowId)   // evita sagas duplicadas para el mismo pedido
                        .build());
        WorkflowClient.start(workflow::run, order);

        Map<String, String> response = new LinkedHashMap<>();
        response.put("orderId", order.getId());
        response.put("workflowId", workflowId);
        response.put("message", "Saga iniciada. Consulte /findorderbyid/" + order.getId());
        return response;
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