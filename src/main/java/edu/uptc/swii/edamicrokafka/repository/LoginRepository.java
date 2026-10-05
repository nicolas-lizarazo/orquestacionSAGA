package edu.uptc.swii.edamicrokafka.repository;

import edu.uptc.swii.edamicrokafka.model.Login;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LoginRepository extends JpaRepository<Login, String> {}