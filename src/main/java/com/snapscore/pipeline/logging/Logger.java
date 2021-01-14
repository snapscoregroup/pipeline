package com.snapscore.pipeline.logging;

import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.function.Function;
import java.util.function.UnaryOperator;

public class Logger {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger("com.snapscore.pipeline");

    /**
     * Provides the values that should be used for the current logging operation
     * IMPORTANT: org.slf4jMDC is a static thread-bound instance. That's why MDC needs to be set before each logging call
     * from scratch and then cleaned up.
     */
    private final MDCProps mdcProps;


    // must not be accessible to ensure a unified way of logging using the methods below
    // also MDCProps is not immutable and we do not want to enable its being shared acrocc multiple JsonLOG instances.
    private Logger(MDCProps mdcProps) {
        this.mdcProps = mdcProps;
    }


    public static Logger woSetup() {
        return new Logger(new MDCProps());
    }

    public static Logger setup(Class<?> clazz) {
        return new Logger(new MDCProps().cls(clazz));
    }

    public static Logger setup(UnaryOperator<MDCProps> mdcProps) {
        try {
            MDCProps mdcPropsNew = mdcProps.apply(new MDCProps());
            return new Logger(mdcPropsNew);
        } catch (Exception e) {
            LOGGER.error("Error setting up logger!", e);
            return Logger.woSetup();
        }
    }

    public Logger decorateSetup(UnaryOperator<MDCProps> mdcPropsToAdd) {
        try {
            MDCProps propsToAdd = mdcPropsToAdd.apply(new MDCProps());
            if (propsToAdd != null) {
                MDCProps mergedProps;
                if (this.mdcProps != null) {
                    mergedProps = this.mdcProps.merge(propsToAdd);
                } else {
                    mergedProps = propsToAdd.copy();
                }
                return new Logger(mergedProps);
            } else {
                return new Logger(new MDCProps());
            }
        } catch (Exception e) {
            LOGGER.error("Error decorating logger!", e);
            return this;
        }
    }



    /**
     * Wraps the actual logging method call with MDC initialisation and MDC and after logging operation call finishes it clears MDC
     *
     * @param logging
     */
    public void log(Function<org.slf4j.Logger, Runnable> logging) {
        try {
            setMDC(mdcProps);
            logging.apply(LOGGER).run();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            clearMDC();
        }
    }

    public void error(String message, Object... arguments) {
        this.log(logger -> () -> logger.error(message, arguments));
    }

    public void warn(String message, Object... arguments) {
        this.log(logger -> () -> logger.warn(message, arguments));
    }

    public void info(String message, Object... arguments) {
        this.log(logger -> () -> logger.info(message, arguments));
    }

    public void debug(String message, Object... arguments) {
        this.log(logger -> () -> logger.debug(message, arguments));
    }

    public void trace(String message, Object... arguments) {
        this.log(logger -> () -> logger.trace(message, arguments));
    }



    private void setMDC(MDCProps mdcProps) {
        if (mdcProps != null) {
            mdcProps.setPropsToMDC();
        }
    }

    private void clearMDC() {
        MDC.clear();
    }

}
