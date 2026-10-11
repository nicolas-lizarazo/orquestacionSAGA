package edu.uptc.swii.edamicrokafka.saga;

import edu.uptc.swii.edamicrokafka.model.Order;
import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface SagaActivities {

    Order createOrder(Order order);

    void validateCustomer(String orderId, String customerDocument);

    void processPayment(String orderId, double total);

    void refundPayment(String orderId);

  
    void confirmOrder(String orderId);

    void cancelOrder(String orderId, String reason);
}