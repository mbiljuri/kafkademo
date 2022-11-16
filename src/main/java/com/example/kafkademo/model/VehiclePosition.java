package com.example.kafkademo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehiclePosition {
    
    private Double xCoordinate;
    private Double yCoordinate;
    private Long id;
    
}
