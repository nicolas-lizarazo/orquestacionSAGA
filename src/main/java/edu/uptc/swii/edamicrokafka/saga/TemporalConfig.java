package edu.uptc.swii.edamicrokafka.saga;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Configuration
public class TemporalConfig {

    @Value("${temporal.target:localhost:7233}")
    private String target;

    @Value("${temporal.namespace:default}")
    private String namespace;

    @Value("${temporal.task-queue:ORDER_SAGA_QUEUE}")
    private String taskQueue;

    @Bean(destroyMethod = "shutdown")
    public WorkflowServiceStubs workflowServiceStubs() {
        return WorkflowServiceStubs.newServiceStubs(
                WorkflowServiceStubsOptions.newBuilder().setTarget(target).build());
    }

    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs) {
        return WorkflowClient.newInstance(stubs,
                WorkflowClientOptions.newBuilder().setNamespace(namespace).build());
    }

    @Bean(destroyMethod = "shutdown")
    public WorkerFactory workerFactory(WorkflowClient client) {
        return WorkerFactory.newInstance(client);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startWorker(ApplicationReadyEvent event) {
        WorkerFactory factory = event.getApplicationContext().getBean(WorkerFactory.class);
        SagaActivitiesImpl activities = event.getApplicationContext().getBean(SagaActivitiesImpl.class);
        Worker worker = factory.newWorker(taskQueue);
        worker.registerWorkflowImplementationTypes(OrderSagaWorkflowImpl.class);
        worker.registerActivitiesImplementations(activities);
        factory.start();
        System.out.println("[Temporal] Worker escuchando en la cola " + taskQueue);
    }
}