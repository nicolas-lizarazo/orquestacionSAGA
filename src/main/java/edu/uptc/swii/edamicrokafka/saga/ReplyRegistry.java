package edu.uptc.swii.edamicrokafka.saga;

import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


@Component
public class ReplyRegistry {

    private final ConcurrentHashMap<String, CompletableFuture<SagaMessage>> pending = new ConcurrentHashMap<>();

    /** Registra la espera ANTES de enviar el comando para no perder la respuesta. */
    public CompletableFuture<SagaMessage> expect(String replyTopic, String orderId) {
        return pending.computeIfAbsent(key(replyTopic, orderId), k -> new CompletableFuture<>());
    }

    public void complete(String replyTopic, SagaMessage reply) {
        pending.computeIfAbsent(key(replyTopic, reply.getOrderId()), k -> new CompletableFuture<>())
                .complete(reply);
    }

    public SagaMessage await(String replyTopic, String orderId, long timeoutSeconds) throws Exception {
        String key = key(replyTopic, orderId);
        try {
            return expect(replyTopic, orderId).get(timeoutSeconds, TimeUnit.SECONDS);
        } finally {
            pending.remove(key);
        }
    }

    private String key(String topic, String orderId) {
        return topic + ":" + orderId;
    }
}