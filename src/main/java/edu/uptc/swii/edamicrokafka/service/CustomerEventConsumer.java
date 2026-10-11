package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Customer;
import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.saga.SagaMessage;
import edu.uptc.swii.edamicrokafka.saga.SagaTopics;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import edu.uptc.swii.edamicrokafka.utils.KafkaEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CustomerEventConsumer {

    @Autowired
    private CustomerService customerService;

    @Autowired
    private LoginService loginService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(topics = "customer_events", groupId = "customer_group")
    public void handleCustomerEvent(String message) {
        KafkaEvent event = JsonUtils.fromJson(message, KafkaEvent.class);
        String operation = event.getOperation();
        String payload = event.getPayload();

        switch (operation) {
            case "ADD":
                Customer newCustomer = JsonUtils.fromJson(payload, Customer.class);
                customerService.save(newCustomer);

                Login login = new Login();
                login.setUsername(newCustomer.getDocument());
                login.setPassword(newCustomer.getDocument());
                loginService.save(login);

                System.out.println("Customer y Login creados para: " + newCustomer.getDocument());
                break;

            case "EDIT":
                Customer editCustomer = JsonUtils.fromJson(payload, Customer.class);
                customerService.save(editCustomer);
                System.out.println("Customer editado: " + editCustomer.getDocument());
                break;

            case "FIND_BY_ID":
                Customer found = customerService.findById(payload);
                System.out.println("Cliente encontrado: " + found);
                break;

            case "FIND_ALL":
                List<Customer> all = customerService.findAll();
                System.out.println("Total clientes: " + all.size());
                break;

            default:
                System.out.println("Operación no soportada en customer_events: " + operation);
        }
    }

    /**
     * SAGA - participante "customer".
     * Recibe validate_customer_cmd y responde OK/FAIL en validate_customer_reply.
     */
    @KafkaListener(topics = SagaTopics.VALIDATE_CUSTOMER_CMD, groupId = "customer_saga_group")
    public void handleValidateCustomer(String message) {
        KafkaEvent event = JsonUtils.fromJson(message, KafkaEvent.class);
        SagaMessage cmd = JsonUtils.fromJson(event.getPayload(), SagaMessage.class);

        SagaMessage reply = SagaMessage.of(cmd.getOrderId());
        try {
            String document = cmd.getCustomerDocument();
            Customer customer = (document == null || document.isBlank())
                    ? null : customerService.findById(document);
            if (customer == null) {
                reply.setStatus(SagaTopics.FAIL);
                reply.setReason("Cliente no válido: no existe el documento " + document);
            } else {
                reply.setStatus(SagaTopics.OK);
            }
        } catch (Exception e) {
            reply.setStatus(SagaTopics.FAIL);
            reply.setReason("Error validando cliente: " + e.getMessage());
        }

        KafkaEvent out = new KafkaEvent(SagaTopics.OP_VALIDATE_CUSTOMER_REPLY, JsonUtils.toJson(reply));
        kafkaTemplate.send(SagaTopics.VALIDATE_CUSTOMER_REPLY, cmd.getOrderId(), JsonUtils.toJson(out));
        System.out.println("[Customer] validate order=" + cmd.getOrderId() + " -> " + reply.getStatus());
    }
}
