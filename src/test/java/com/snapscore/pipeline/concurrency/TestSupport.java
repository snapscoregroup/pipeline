package com.snapscore.pipeline.concurrency;

class TestSupport {

    static class TestMessage {
        public final int entityId;
        public final int messageNo;

        public TestMessage(int entityId, int messageNo) {
            this.entityId = entityId;
            this.messageNo = messageNo;
        }

        @Override
        public String toString() {
            return "TestMessage{" +
                    "entityId=" + entityId +
                    ", messageNo=" + messageNo +
                    '}';
        }
    }

    static class TestInputQueueResolver extends InputQueueResolver<TestMessage> {
        @Override
        public int getQueueIdxFor(TestMessage input, int inputQueueCount) {
            return calcIdx(inputQueueCount, input.entityId);
        }
    }

}
