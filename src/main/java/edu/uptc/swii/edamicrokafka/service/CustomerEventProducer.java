package edu.uptc.swii.edamicrokafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import edu.uptc.swii.edamicrokafka.model.Customer;
import edu.uptc.swii.edamicrokafka.utils.JsonUtils;

@Service
public class CustomerEventProducer {
    private static final String TOPIC_ADD = "addcustomer_events";
    private static final String TOPIC_EDIT = "editcustomer_events";
    private static final String TOPIC_FINDBYID = "findcustomerbyid_events";
    private static final String TOPIC_FINDALLCUSTOMERS = "findallcustomers_events";
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateAdd;

    public void sendAddCustomerEvent(Customer customer) {
        String json = null;
        JsonUtils jsonUtils = new JsonUtils();
        json = jsonUtils.toJson(customer);
        kafkaTemplateAdd.send(TOPIC_ADD, json);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateEdit;

    public void sendEditCustomerEvent(Customer customer) {
        String json = null;
        JsonUtils jsonUtils = new JsonUtils();
        json = jsonUtils.toJson(customer);
        kafkaTemplateEdit.send(TOPIC_EDIT, json);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateFindById;

    public void sendFindByCustomerIDEvent(String document) {
        kafkaTemplateFindById.send(TOPIC_FINDBYID, document);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplateFindAllOrders;

    public void sendFindAllOrdersEvent(String customers) {
        kafkaTemplateFindAllOrders.send(TOPIC_FINDALLCUSTOMERS, customers);
    }
}