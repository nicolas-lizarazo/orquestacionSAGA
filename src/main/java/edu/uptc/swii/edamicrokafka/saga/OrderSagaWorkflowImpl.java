package edu.uptc.swii.edamicrokafka.saga;

import edu.uptc.swii.edamicrokafka.model.Order;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Saga;
import io.temporal.workflow.Workflow;
import org.slf4j.Logger;

import java.time.Duration;

public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {

    private static final Logger log = Workflow.getLogger(OrderSagaWorkflowImpl.class);

    private final SagaActivities activities = Workflow.newActivityStub(
            SagaActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2.0)
                            .setMaximumAttempts(3)
                            // una respuesta FAIL del participante no se reintenta
                            .setDoNotRetry(SagaActivitiesImpl.BUSINESS_FAILURE)
                            .build())
                    .build());

    @Override
    public String run(Order order) {
        Saga saga = new Saga(new Saga.Options.Builder()
                .setParallelCompensation(false)
                .setContinueWithError(true) 
                .build());
        String orderId = order.getId();

        try {
            activities.createOrder(order);

            activities.validateCustomer(orderId, order.getCustomerDocument());

            saga.addCompensation(activities::refundPayment, orderId);
            activities.processPayment(orderId, order.getTotal());

            activities.confirmOrder(orderId);
            return "CONFIRMED";

        } catch (ActivityFailure e) {
            String reason = rootMessage(e);
            log.warn("Saga fallida para order {}: {}. Ejecutando compensaciones.", orderId, reason);
            saga.compensate();                      
            activities.cancelOrder(orderId, reason);
            return "CANCELLED";
        }
    }

    private String rootMessage(ActivityFailure e) {
        Throwable cause = e.getCause();
        if (cause instanceof ApplicationFailure af) {
            return af.getOriginalMessage();
        }
        return e.getMessage();
    }
}