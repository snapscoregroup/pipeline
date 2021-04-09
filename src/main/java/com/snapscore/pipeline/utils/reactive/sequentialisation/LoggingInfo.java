package com.snapscore.pipeline.utils.reactive.sequentialisation;

import com.snapscore.pipeline.logging.Logger;

import java.util.function.Function;

public class LoggingInfo {

    public final String inputDescription;
    private final Function<Logger, Logger> loggerDecorator;

    public LoggingInfo(String inputDescription, Function<Logger, Logger> loggerDecorator) {
        this.inputDescription = inputDescription;
        this.loggerDecorator = loggerDecorator;
    }

    public LoggingInfo(String inputDescription) {
        this(inputDescription, null);
    }

    public Logger decorate(Logger logger) {
        if (loggerDecorator != null) {
            return loggerDecorator.apply(logger);
        } else {
            return logger;
        }
    }

}
