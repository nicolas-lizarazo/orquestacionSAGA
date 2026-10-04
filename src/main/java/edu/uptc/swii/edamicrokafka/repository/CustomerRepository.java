package edu.uptc.swii.edamicrokafka.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import edu.uptc.swii.edamicrokafka.model.Customer;

public interface CustomerRepository extends JpaRepository<Customer, String> {
}