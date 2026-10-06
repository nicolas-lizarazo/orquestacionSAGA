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
}
