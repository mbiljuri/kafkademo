package com.example.kafkademo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DistanceInfo {

    private Double distance;
    private VehiclePosition currentVehiclePos;
    
}
