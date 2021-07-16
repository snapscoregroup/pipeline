package com.snapscore.pipeline.concurrency;

import com.snapscore.pipeline.logging.Logger;

import java.util.function.Function;

/**
 * Logging information that will be used by the internals of the data processing execution
 */
public class LoggingInfo {

    public final boolean logActivity;
    public final String inputDescription;
    private final Function<Logger, Logger> loggerDecorator;

    public LoggingInfo(boolean logActivity, String inputDescription, Function<Logger, Logger> loggerDecorator) {
        this.logActivity = logActivity;
        this.inputDescription = inputDescription;
        this.loggerDecorator = loggerDecorator;
    }

    public LoggingInfo(boolean logActivity, String inputDescription) {
        this(logActivity, inputDescription, null);
    }

    public Logger decorate(Logger logger) {
        if (loggerDecorator != null) {
            return loggerDecorator.apply(logger);
        } else {
            return logger;
        }
    }

}
