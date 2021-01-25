package com.snapscore.pipeline.utils.reactive.sequentialisation;

import com.snapscore.pipeline.logging.Logger;

import java.util.function.Function;

public class LoggingInfo {

    private final String message;
    private final Function<Logger, Logger> loggerDecorator;

    public LoggingInfo(String message, Function<Logger, Logger> loggerDecorator) {
        this.message = message;
        this.loggerDecorator = loggerDecorator;
    }

    public LoggingInfo(String message) {
        this(message, null);
    }

    public Logger decorate(Logger logger) {
        if (loggerDecorator != null) {
            return loggerDecorator.apply(logger);
        } else {
            return logger;
        }
    }

    public String getMessage() {
        return message;
    }
}
