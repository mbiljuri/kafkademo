package com.example.kafkademo.api.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.kafkademo.model.DistanceInfo;
import com.example.kafkademo.model.VehiclePosition;

@Service
public class KafkaService {

    private Map<Long, DistanceInfo> distances = new HashMap<>();

    @Value("${kafka.input.topic}")
    private String inputTopic;
    @Value("${kafka.output.topic}")
    private String outputTopic;
    private KafkaTemplate<String, VehiclePosition> kafkaTemplateInputTopic;
    private KafkaTemplate<String, DistanceInfo> kafkaTemplateOutputTopic;

    public KafkaService(KafkaTemplate<String, VehiclePosition> kafkaTemplateInputTopic,
            KafkaTemplate<String, DistanceInfo> kafkaTemplateOutputTopic) {
        this.kafkaTemplateInputTopic = kafkaTemplateInputTopic;
        this.kafkaTemplateOutputTopic = kafkaTemplateOutputTopic;
    }

    public void sendVehiclePosition(VehiclePosition vehicle) {
        kafkaTemplateInputTopic.send(inputTopic, vehicle);
    }

    public Map<Long, DistanceInfo> getDistances() {
        return distances;
    }

    @KafkaListener(topics = "input", groupId = "vehicle-group")
    private void consumeVehiclePositons1(VehiclePosition vehiclePosition) {
        if (distances.containsKey(vehiclePosition.getId())) {
            DistanceInfo distanceInfo = distances.get(vehiclePosition.getId());
            Double newDistance = calculateDistance(vehiclePosition, distanceInfo.getCurrentVehiclePos())
                    + distanceInfo.getDistance();
            distanceInfo.setDistance(newDistance);
            distanceInfo.setCurrentVehiclePos(vehiclePosition);
            distances.put(vehiclePosition.getId(), distanceInfo);
        } else {
            Double distance = calculateDistance(null, vehiclePosition);
            DistanceInfo distanceInfo = new DistanceInfo(distance, vehiclePosition);
            distances.put(vehiclePosition.getId(), distanceInfo);
        }

        kafkaTemplateOutputTopic.send(outputTopic, distances.get(vehiclePosition.getId()));
    }

    @KafkaListener(topics = "input", groupId = "vehicle-group")
    private void consumeVehiclePositons2(VehiclePosition vehiclePosition) {
        if (distances.containsKey(vehiclePosition.getId())) {
            DistanceInfo distanceInfo = distances.get(vehiclePosition.getId());
            Double newDistance = calculateDistance(vehiclePosition, distanceInfo.getCurrentVehiclePos())
                    + distanceInfo.getDistance();
            distanceInfo.setDistance(newDistance);
            distanceInfo.setCurrentVehiclePos(vehiclePosition);
            distances.put(vehiclePosition.getId(), distanceInfo);
        } else {
            Double distance = calculateDistance(null, vehiclePosition);
            DistanceInfo distanceInfo = new DistanceInfo(distance, vehiclePosition);
            distances.put(vehiclePosition.getId(), distanceInfo);
        }

        kafkaTemplateOutputTopic.send(outputTopic, distances.get(vehiclePosition.getId()));
    }

    @KafkaListener(topics = "input", groupId = "vehicle-group")
    private void consumeVehiclePositons3(VehiclePosition vehiclePosition) {
        if (distances.containsKey(vehiclePosition.getId())) {
            DistanceInfo distanceInfo = distances.get(vehiclePosition.getId());
            Double newDistance = calculateDistance(vehiclePosition, distanceInfo.getCurrentVehiclePos())
                    + distanceInfo.getDistance();
            distanceInfo.setDistance(newDistance);
            distanceInfo.setCurrentVehiclePos(vehiclePosition);
            distances.put(vehiclePosition.getId(), distanceInfo);
        } else {
            Double distance = calculateDistance(null, vehiclePosition);
            DistanceInfo distanceInfo = new DistanceInfo(distance, vehiclePosition);
            distances.put(vehiclePosition.getId(), distanceInfo);
        }

        kafkaTemplateOutputTopic.send(outputTopic, distances.get(vehiclePosition.getId()));
    }

    @KafkaListener(topics = "output", groupId = "vehicle-group")
    private void displayDistance(DistanceInfo distanceInfo) {
        System.out.printf("Distance travelled for vehicle id: %s is %s", distanceInfo.getCurrentVehiclePos().getId(), distanceInfo.getDistance());
        System.out.println();
    }

    private Double calculateDistance(VehiclePosition pos1, VehiclePosition pos2) {
        if (pos1 == null)
            pos1 = new VehiclePosition(0.0, 0.0, 0L);
        return Math.sqrt((pos2.getYCoordinate() - pos1.getYCoordinate())
                * (pos2.getYCoordinate() - pos1.getYCoordinate())
                + (pos2.getXCoordinate() - pos1.getXCoordinate()) * (pos2.getXCoordinate() - pos1.getXCoordinate()));
    }

}
