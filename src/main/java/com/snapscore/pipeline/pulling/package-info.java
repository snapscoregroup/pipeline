/**
 * Package contains classes for pulling management: <br>
 * <br> 1) pull a request only once
 * <br> 2) schedule pulling of a specific request(s) at given intervals
 * <br> 3) retry of any request that has failed for a preconfigured amount of times & retry intervals
 * <br> 4) ability to track all scheduled requests, groups of requests or all requests identified by their <code>scheduledPullingKey</code> inside {@link com.snapscore.pipeline.pulling.ScheduledPullingCache}.
 * This is useful to e.g. change frequency of pulling of existing requests or cancel specific requests pulling.
 * <br> 5) schedule pulling of 'dynamically created requests' - e.g. when url changes for every request when it contains timestamps.
 * <br> 6) consume response http client data as byte[] so that we can process documents as well as images
 *
 * @see com.snapscore.pipeline.pulling.PullingScheduler
 * @see com.snapscore.pipeline.pulling.ScheduledPullingCache
 */
package com.snapscore.pipeline.pulling;