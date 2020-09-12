package com.hahooy.dataloader_demo;

import org.dataloader.DataLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Dispatcher {
    private final List<DataLoader<?, ?>> dataLoaders;

    private Dispatcher(List<DataLoader<?, ?>> dataLoaders) {
        this.dataLoaders = dataLoaders;
    }

    private int depth() {
        return dataLoaders.stream()
                .mapToInt(DataLoader::dispatchDepth)
                .sum();
    }

    /**
     * Dispatch all pending data loaders, wait for the futures returned from the dispatched
     * data loaders to complete and then repeat the process to dispatch new pending data loaders
     * that come from potential thenCompose chaining. This process can be repeated until all levels
     * of pending data loaders are dispatched.
     *
     * In every round of dispatch, this will be able to batch all tasks whose dependencies have been
     * resolved by previous dispatches.
     */
    void dispatchAllAndJoin() {
        while (depth() > 0) {
            // Dispatch all data loaders. This will kick off all batched async tasks.
            CompletableFuture<?>[] futures = dataLoaders.stream()
                    .filter(dataLoader -> dataLoader.dispatchDepth() > 0)
                    .map(DataLoader::dispatch)
                    .toArray(CompletableFuture[]::new);
            // Wait for the futures to complete.
            Utils.log("Start waiting for futures");
            CompletableFuture.allOf(futures).join();
            Utils.log("Finish waiting for futures");
        }
    }

    static DispatcherBuilder builder() {
        return new DispatcherBuilder();
    }

    public static final class DispatcherBuilder {
        List<DataLoader<?, ?>> dataLoaders = new ArrayList<>();

        private DispatcherBuilder() {
        }

        public DispatcherBuilder withDataLoaders(List<DataLoader<?, ?>> dataLoaders) {
            this.dataLoaders = dataLoaders;
            return this;
        }

        public DispatcherBuilder addDataLoader(DataLoader<?, ?> dataLoader) {
            this.dataLoaders.add(dataLoader);
            return this;
        }

        public Dispatcher build() {
            return new Dispatcher(dataLoaders);
        }
    }
}
