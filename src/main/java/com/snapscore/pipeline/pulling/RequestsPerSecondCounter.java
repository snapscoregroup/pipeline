package com.snapscore.pipeline.pulling;

import java.time.LocalDateTime;

public interface RequestsPerSecondCounter {

    boolean incrementIfRequestWithinLimitAndGet(LocalDateTime now);

}
