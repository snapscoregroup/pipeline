/**
 * The functionality in this package makes it possible to process data in fully parallel fashion while
 * also making sure that the order of processing is correct
 *
 * <br>
 * <br>
 *
 * USE CASES:
 *
 * <br>
 * <br>
 *
 * 1. Correct ordering
 * When we want to process e.g. match data in parallel but also preserve the order for individual matches
 * we can use {@link com.snapscore.pipeline.concurrency.ConcurrentSequentialProcessor} so that all data
 * for a single match gets processed in the order in which it was submitted for processing sequentially
 *
 * <br>
 * <br>
 *
 * 2. Thread safety
 * When we update data of an entity (e.g. a match) sequentially we remove the concurrency only that might otherwise lead to concurrency problems - data races and such ...
 * Data for multiple entities (e.g. different matches) can proceed in parallel but the processing of multiple updates/inputs for the same entity
 * will be strictly sequential so there are no concurrency problems with respect to that entities shared state.
 * Of course if there are data structures involved and used by all entities, those still need to be implemented in a thead safe way.
 *
 * <br>
 * <br>
 *
 * 3. Performance
 * We can separate blocking and non-blocking code execution so that they run on different thread pools to make the most of the processing power we have
 * The definition of what will be executed by the {@link com.snapscore.pipeline.concurrency.ConcurrentSequentialProcessor}
 * is defined by the implementation of {@link com.snapscore.pipeline.concurrency.InputProcessingRunner}.
 * We can create chained async operations in both {@link com.snapscore.pipeline.concurrency.InputProcessingFluxRunner}
 * and {@link com.snapscore.pipeline.concurrency.InputProcessingCallableRunner}
 */
package com.snapscore.pipeline.concurrency;
