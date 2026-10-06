package edu.uptc.swii.edamicrokafka.controller;

import edu.uptc.swii.edamicrokafka.service.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import edu.uptc.swii.edamicrokafka.model.Customer;
import edu.uptc.swii.edamicrokafka.service.CustomerEventProducer;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;

import java.util.List;

@RestController
public class CustomerController {
    @Autowired
    private CustomerEventProducer customerEventProducer;

    @Autowired
    private CustomerService customerService;

    private static JsonUtils jsonUtils = new JsonUtils();

    @PostMapping("/addcustomer")
    public String sendMessageAddCustomer(@RequestBody String customer) {
        Customer customerObj = new Customer();
        customerObj = jsonUtils.fromJson(customer, Customer.class);
        customerEventProducer.sendAddCustomerEvent(customerObj);
        return "Evento enviado";
    }
    @PostMapping("/editcustomer")
    public String sendMessageEditCustomer(@RequestBody String customer) {
        Customer customerObj = new Customer();
        customerObj = jsonUtils.fromJson(customer, Customer.class);
        customerEventProducer.sendEditCustomerEvent(customerObj);
        return "Evento Enviado";
    }

    @GetMapping("/findcustomerbyid/{document}")
    public Customer findById(@PathVariable String document) {
        return customerService.findById(document);
    }

    @GetMapping("/findallcustomers")
    public List<Customer> findAll() {
        return customerService.findAll();
    }
}