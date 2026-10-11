package edu.uptc.swii.edamicrokafka.saga;

import edu.uptc.swii.edamicrokafka.model.Order;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderSagaWorkflow {

    @WorkflowMethod
    String run(Order order);
}