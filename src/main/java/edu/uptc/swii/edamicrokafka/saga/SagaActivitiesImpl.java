package edu.uptc.swii.edamicrokafka.saga;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.service.OrderService;
import io.temporal.failure.ApplicationFailure;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeoutException;

@Component
public class SagaActivitiesImpl implements SagaActivities {

    public static final String BUSINESS_FAILURE = "BUSINESS_FAILURE";

    private final OrderService orderService;
    private final SagaPublisher publisher;
    private final ReplyRegistry replies;

    @Value("${saga.reply-timeout-seconds:10}")
    private long replyTimeout;

    public SagaActivitiesImpl(OrderService orderService, SagaPublisher publisher, ReplyRegistry replies) {
        this.orderService = orderService;
        this.publisher = publisher;
        this.replies = replies;
    }

    @Override
    public Order createOrder(Order order) {
        order.setStatus("PENDING");
        order.setStatusReason(null);
        System.out.println("[Saga] 1. Order " + order.getId() + " creada en PENDING");
        return orderService.save(order);
    }

    @Override
    public void validateCustomer(String orderId, String customerDocument) {
        SagaMessage cmd = SagaMessage.of(orderId);
        cmd.setCustomerDocument(customerDocument);
        requestReply(SagaTopics.VALIDATE_CUSTOMER_CMD, SagaTopics.OP_VALIDATE_CUSTOMER,
                SagaTopics.VALIDATE_CUSTOMER_REPLY, cmd, "Cliente no válido");
        System.out.println("[Saga] 2. Cliente validado para order " + orderId);
    }

    @Override
    public void processPayment(String orderId, double total) {
        SagaMessage cmd = SagaMessage.of(orderId);
        cmd.setTotal(total);
        requestReply(SagaTopics.PAYMENT_CMD, SagaTopics.OP_PAY,
                SagaTopics.PAYMENT_REPLY, cmd, "Pago rechazado");
        System.out.println("[Saga] 3. Pago aprobado para order " + orderId);
    }

    @Override
    public void refundPayment(String orderId) {
        publisher.publish(SagaTopics.PAYMENT_REFUND_CMD, SagaTopics.OP_REFUND, SagaMessage.of(orderId));
        System.out.println("[Saga] COMPENSACION: reembolso solicitado para order " + orderId);
    }

    @Override
    public void confirmOrder(String orderId) {
        SagaMessage msg = SagaMessage.of(orderId);
        msg.setStatus("CONFIRMED");
        publisher.publish(SagaTopics.ORDER_EVENTS, SagaTopics.OP_STATUS_UPDATE, msg);
    }

    @Override
    public void cancelOrder(String orderId, String reason) {
        SagaMessage msg = SagaMessage.of(orderId);
        msg.setStatus("CANCELLED");
        msg.setReason(reason);
        publisher.publish(SagaTopics.ORDER_EVENTS, SagaTopics.OP_STATUS_UPDATE, msg);
        System.out.println("[Saga] COMPENSACION: order " + orderId + " cancelada (" + reason + ")");
    }

    private void requestReply(String cmdTopic, String operation, String replyTopic,
                              SagaMessage cmd, String failMessage) {
        replies.expect(replyTopic, cmd.getOrderId());          
        publisher.publish(cmdTopic, operation, cmd);
        try {
            SagaMessage reply = replies.await(replyTopic, cmd.getOrderId(), replyTimeout);
            if (!SagaTopics.OK.equals(reply.getStatus())) {
                String reason = reply.getReason() != null ? reply.getReason() : failMessage;
                throw ApplicationFailure.newNonRetryableFailure(reason, BUSINESS_FAILURE);
            }
        } catch (TimeoutException e) {
            throw ApplicationFailure.newFailure(
                    "Sin respuesta en " + replyTopic + " tras " + replyTimeout + "s", "REPLY_TIMEOUT");
        } catch (ApplicationFailure e) {
            throw e;
        } catch (Exception e) {
            throw ApplicationFailure.newFailure(e.getMessage(), "REPLY_ERROR");
        }
    }
}