package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Customer;
import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;
import edu.uptc.swii.edamicrokafka.utils.KafkaEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CustomerEventConsumer {

    @Autowired
    private CustomerService customerService;

    // CAMBIO (Punto 3): inyectar LoginService para crear el registro Login
    @Autowired
    private LoginService loginService;

    // CAMBIO (Punto 4): un único listener para todos los eventos de Customer
    @KafkaListener(topics = "customer_events", groupId = "customer_group")
    public void handleCustomerEvent(String message) {
        KafkaEvent event = JsonUtils.fromJson(message, KafkaEvent.class);
        String operation = event.getOperation();
        String payload = event.getPayload();

        switch (operation) {
            case "ADD":
                Customer newCustomer = JsonUtils.fromJson(payload, Customer.class);
                customerService.save(newCustomer);

                // CAMBIO (Punto 3): crear Login asociado al Customer
                Login login = new Login();
                login.setUsername(newCustomer.getDocument());
                login.setPassword(newCustomer.getDocument()); // contraseña por defecto = documento
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
}