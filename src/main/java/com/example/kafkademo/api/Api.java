package com.example.kafkademo.api;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkademo.api.service.KafkaService;
import com.example.kafkademo.model.VehiclePosition;

import lombok.AllArgsConstructor;

@RestController
@RequestMapping(path = "api/v1/taxi", produces = MediaType.APPLICATION_JSON_VALUE)
@AllArgsConstructor
public class Api {

    private final KafkaService kafkaService;

    @PostMapping(path = "signal/submit")
    public void submitVehicle(@RequestBody VehiclePosition vehicle) {
        kafkaService.sendVehiclePosition(vehicle);
    }
    
}
