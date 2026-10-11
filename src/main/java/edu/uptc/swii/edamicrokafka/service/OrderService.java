package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Order;
import edu.uptc.swii.edamicrokafka.repository.OrderRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderService {
    private final OrderRepository repo;

    public OrderService(OrderRepository repo) {
        this.repo = repo;
    }

    public Order save(Order order) {
        return repo.save(order);
    }

    public Order findById(String id){
        return repo.findById(id).orElse(null);
    }

    public List<Order> findAll(){
        return repo.findAll();
    }

    /** Actualiza el estado de un pedido (idempotente). */
    public Order updateStatus(String id, String status, String reason) {
        Order order = repo.findById(id).orElse(null);
        if (order == null) {
            return null;
        }
        order.setStatus(status);
        order.setStatusReason(reason);
        return repo.save(order);
    }
}