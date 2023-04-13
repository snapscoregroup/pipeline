package com.snapscore.pipeline.logging;

import org.junit.Test;

import java.util.Map;

public class LoggerExamplesTest {

    // example class-level basic setup
    private static final Logger logger = Logger.setup(LoggerExamplesTest.class);

    // ================================================
    //
    // rename resources/logback_example.xml to resources/logback.xml to try the examples
    //
    // ================================================


    @Test
    public void multipleLoggingCallsHaveCommonMDCProps() throws InterruptedException {

        // MDC initialise some general settings (e.g. could be used in actor instances used for a specific event)
        Logger logger = Logger.setup(mdc -> mdc.comp("some component").prov("ENET").sp("SOCCER"));

        logger.info("processing ...");


        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void multipleLoggingCallsHaveCommonMDCPropsWithDecoration() throws InterruptedException {

        // MDC initialise some general settings (e.g. could be used in actor instances used for a specific event)
        Logger logger = Logger.setup(mdc -> mdc.comp("some component").prov("ENET").sp("SOCCER"));

        logger.info("processing ...");
        logger.decorateSetup(mdc -> mdc.stgIdEnet(1234456)).info("processing ...");
        logger.decorateSetup(mdc -> mdc.stgIdEnet(9876543)).info("processing ...");

        // basic jsonLog should not be affected by previous decoration
        logger.info("processing ...");

        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void loggingCallsHaveSpecificMDCPropsEveryTime() throws InterruptedException {

        Logger.setup(mdc -> mdc.stgIdEnet("861345").evTy("STAGE"))
                .info("Processing stage ...");

        Logger.setup(mdc -> mdc.stgIdEnet("879398").eIdEnet("32009393").evTy("FULL_MATCH_DETAILS"))
                .info("Processing stage event ...");

        Logger.setup(mdc -> mdc.cls(LoggerExamplesTest.class).stgIdEnet("847563").eIdEnet("34309393").evTy("FULL_MATCH_DETAILS"))
                .info("Processing stage event ...");

        try {
            @SuppressWarnings("divzero") int i = 1 /0;
        } catch (Exception e) {
            Logger.setup(mdc -> mdc.cls(LoggerExamplesTest.class).stgIdEnet("847563").eIdEnet("34309393").evTy("FULL_MATCH_DETAILS"))
                    .error("Processing stage event ... attempt no {}", 2, e);
        }

        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void loggingProviderMappedEventIds() throws InterruptedException {

        Logger.setup(mdc -> mdc.stgIdEnet("861345").eIds(Map.of(1, "353553535", 2, "56561664")))
                .info("Processing stage ...");

        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void loggingProviderMappedEventIdsDecoratedByAdditionalEventIds() throws InterruptedException {

        Logger logger = Logger.setup(props -> props.eIdEnet("861345"));
        logger.info("Processing event ...");

        Logger loggerDecorated = logger.decorateSetup(props -> props.eIdInternal("123"));
        loggerDecorated.info("Processing internal event ...");

        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void loggingTeamIdsDecoratedByAdditionalTeamIds() throws InterruptedException {

        Logger logger = Logger.setup(props -> props.tIdEnet("861345"));
        logger.info("Processing team ...");

        Logger loggerDecorated = logger.decorateSetup(props -> props.tIdInternal("123"));
        loggerDecorated.info("Processing internal team ...");

        Logger loggerDecorated2 = loggerDecorated.decorateSetup(props -> props.comp("some component"));
        loggerDecorated2.info("Processing internal team 2 ...");

        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void loggingProviderTeamIds() throws InterruptedException {

        Logger.setup(mdc -> mdc.stgIdEnet("861345").tIdEnet("9584844").tIdEnet(988484))
                .info("Processing enet teams ...");


        Thread.sleep(1000); // let logging get written to file
    }

    @Test
    public void loggingAnyIds() throws InterruptedException {

        Logger.setup(mdc -> mdc.anyId("FMKMSKMF").anyId("FMK454545SD").anyId(565959959))
                .info("Processing requests with uuids ... ...");


        Thread.sleep(1000); // let logging get written to file
    }


    @Test
    public void loggingWithAccessingTheLoggerDirectly() throws InterruptedException {

        Logger log = Logger.woSetup();
        log.log(logger -> () -> logger.info("something is happening for {}th time", 5));

        Throwable error = new Throwable("some horrible error"); // dummy error ...
        log.log(logger -> () -> logger.error("some error", error));

        Thread.sleep(1000); // let logging get written to file
    }

}
