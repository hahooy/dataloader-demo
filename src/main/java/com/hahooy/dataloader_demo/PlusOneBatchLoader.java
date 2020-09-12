package com.hahooy.dataloader_demo;

import org.dataloader.BatchLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class PlusOneBatchLoader implements BatchLoader<Double, Double> {

    @Override
    public CompletionStage<List<Double>> load(List<Double> numbers) {
        Utils.log("Start computing plus one for numbers: " + numbers);
        return CompletableFuture.supplyAsync(() -> {
            // Sleep for a random period of time to simulate the latency of
            // making a service call over http.
            Utils.sleepRandom("PlusOneBatchLoader", 1_00, 5_00);
            return numbers.stream()
                    .map(num -> num != null ? num + 1 : null)
                    .collect(Collectors.toList());
        });
    }
}
