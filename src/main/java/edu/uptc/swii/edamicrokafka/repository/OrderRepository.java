package edu.uptc.swii.edamicrokafka.repository;

import edu.uptc.swii.edamicrokafka.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order,String > {}
