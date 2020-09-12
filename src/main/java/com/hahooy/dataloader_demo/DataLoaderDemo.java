package com.hahooy.dataloader_demo;

import org.dataloader.DataLoader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class DataLoaderDemo {

    void demoDataLoader() throws ExecutionException, InterruptedException {
        Utils.log("Demo started");
        var squareDl = DataLoader.newDataLoader(new SquareBatchLoader());
        var sqrtDl = DataLoader.newDataLoader(new SqrtBatchLoader());
        var plusOneDl = DataLoader.newDataLoader(new PlusOneBatchLoader());
        var eleven = 11.0;
        var twelve = 12.0;

        CompletableFuture<Double> squarePlusOneResultFor11 = squareDl.load(eleven)
                .thenCompose(val -> plusOne(plusOneDl, val));
        CompletableFuture<Double> squarePlusOneResultFor12 = squareDl.load(twelve)
                .thenCompose(val -> plusOne(plusOneDl, val));
        CompletableFuture<Double> sqrtPlusOneResultFor11 = sqrtDl.load(eleven)
                .thenCompose(val -> plusOne(plusOneDl, val));
        CompletableFuture<Double> squarePlusOneAndSquareResultFor11 = squareDl.load(eleven)
                .thenApply(val -> val + 1)
                .thenCompose(val -> plusOne(plusOneDl, val))
                .thenCompose(squareDl::load);
        var dispatcher = Dispatcher.builder()
                .addDataLoader(plusOneDl)
                .addDataLoader(squareDl)
                .addDataLoader(sqrtDl)
                .build();
        // Need to dispatch all data loaders before calling .get on futures otherwise
        // the current thread will be blocked and never be able to make progress.
        dispatcher.dispatchAllAndJoin();
        // At this point all computations should be completed. Computations using the same
        // data loader should be batched.
        Utils.log(String.format("The square of %.2f plus one is: %.2f", eleven, squarePlusOneResultFor11.get()));
        Utils.log(String.format("The square of %.2f plus one is: %.2f", twelve, squarePlusOneResultFor12.get()));
        Utils.log(String.format("The square root of %.2f plus one is: %.2f", eleven, sqrtPlusOneResultFor11.get()));
        Utils.log(String.format("The square of %.2f plus one and then square again is: %.2f", eleven, squarePlusOneAndSquareResultFor11.get()));
        Utils.log("Demo finished");
    }

    private CompletionStage<Double> plusOne(DataLoader<Double, Double> plusOneDl, Double val) {
        Utils.sleepRandom("DataLoaderDemo#plusOne", 1_000, 10_000);
        Utils.log(String.format("Adding %.2f to plus one data loader", val));
        return plusOneDl.load(val);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var demo = new DataLoaderDemo();
        demo.demoDataLoader();
    }
}
