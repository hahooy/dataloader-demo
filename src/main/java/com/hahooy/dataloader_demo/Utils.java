package com.hahooy.dataloader_demo;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Utils {
    private static final Logger LOGGER = LogManager.getLogger("DataLoaderDemo");

    static void sleepRandom(String caller, int millisLow, int millisHi) {
        try {
            long millisToSleep = (long) (millisLow + (millisHi - millisLow) * Math.random());
            log(String.format("%s sleeps for %.2f seconds", caller, millisToSleep / 1000.0));
            Thread.sleep(millisToSleep);
        } catch (InterruptedException ignored) {
            // no-op
        }
    }

    public static void log(Object msg) {
        LOGGER.info(msg);
    }
}
