package com.snapscore.pipeline.utils.reactive.sequentialisation;

class TestSupport {

    static class TestMessage {
        public final int entityId;
        public final int messageNo;

        public TestMessage(int entityId, int messageNo) {
            this.entityId = entityId;
            this.messageNo = messageNo;
        }
    }

    static class TestQueueResolver extends QueueResolver<TestMessage> {
        @Override
        public int getQueueIdxFor(TestMessage input, int inputQueueCount) {
            return calcIdx(inputQueueCount, input.entityId);
        }
    }

}
