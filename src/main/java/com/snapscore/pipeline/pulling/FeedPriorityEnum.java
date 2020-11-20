package com.snapscore.pipeline.pulling;

public enum FeedPriorityEnum {

    HIHGEST(1), // for live matches that we do not want to be blocked by whole-season data pulling
    HIGH(2),
    MEDIUM(3), // for narrow date range around today
    LOW(4), // for relatively important feeds but which require a lot of requests
    LOWEST(5) // for anything else that e.g. produces a lot of requests and is not needed to be pulled as fast as possible when the app starts
    ;

    // used for prioritizing the requests
    private final int schedulingOrder;

    FeedPriorityEnum(int schedulingOrder) {
        this.schedulingOrder = schedulingOrder;
    }

    public int getSchedulingOrder() {
        return schedulingOrder;
    }
}
