package edu.uptc.swii.edamicrokafka.service;

import edu.uptc.swii.edamicrokafka.model.Login;
import edu.uptc.swii.edamicrokafka.repository.LoginRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LoginService {
    private final LoginRepository repo;

    public LoginService(LoginRepository repo) {
        this.repo = repo;
    }

    public Login save(Login l) {
        return repo.saveAndFlush(l);
    }

    public Login findById(String username) {
        return repo.findById(username).orElse(null);
    }

    public List<Login> findAll() {
        return repo.findAll();
    }
}
