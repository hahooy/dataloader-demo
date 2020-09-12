package com.hahooy.dataloader_demo;

import org.dataloader.BatchLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class SquareBatchLoader implements BatchLoader<Double, Double> {

    @Override
    public CompletionStage<List<Double>> load(List<Double> numbers) {
        Utils.log("Start computing square for numbers: " + numbers);
        return CompletableFuture.supplyAsync(() -> {
            // Sleep for a random period of time to simulate the latency of
            // making a service call over http.
            Utils.sleepRandom("SquareBatchLoader", 5_000, 10_000);
            return numbers.stream()
                    .map(num -> num != null ? Math.pow(num, 2) : null)
                    .collect(Collectors.toList());
        });
    }
}
