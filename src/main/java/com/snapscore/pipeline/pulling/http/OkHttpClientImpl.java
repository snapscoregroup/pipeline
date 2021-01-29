package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.logging.Logger;
import com.snapscore.pipeline.pulling.FeedPriorityEnum;
import com.snapscore.pipeline.pulling.FeedRequest;
import com.snapscore.pipeline.pulling.FeedRequestHttpHeader;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.Request;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OkHttpClientImpl implements HttpClient {

    private static final Logger logger = Logger.setup(OkHttpClientImpl.class);

    private final Map<FeedPriorityEnum, okhttp3.OkHttpClient> clientsByPriorityMap = new HashMap<>();
    private final List<ExecutorService> clientExecutorServices = new ArrayList<>();
    private final ClientCallbackFactory<OkHttpClientCallback> clientCallbackFactory;

    public OkHttpClientImpl(HttpClientConfig httpClientConfig,
                            ClientCallbackFactory<OkHttpClientCallback> clientCallbackFactory) {
        this.clientCallbackFactory = clientCallbackFactory;
        for (FeedPriorityEnum feedPriority : FeedPriorityEnum.values()) {
            ExecutorService executorService = Executors.newFixedThreadPool(httpClientConfig.getNumberOfThreads());
            clientExecutorServices.add(executorService);
            Dispatcher dispatcher = new Dispatcher(executorService);
            okhttp3.OkHttpClient okHttpClient = new okhttp3.OkHttpClient.Builder()
                    .readTimeout(httpClientConfig.readTimeout().getSeconds(), TimeUnit.SECONDS)
                    .connectionPool(new ConnectionPool(100, 5, TimeUnit.MINUTES))
                    .dispatcher(dispatcher)
                    .build();
            clientsByPriorityMap.put(feedPriority, okHttpClient);
        }
    }

    @Override
    public CompletableFuture<byte[]> getAsync(FeedRequest feedRequest) {
        Mono<byte[]> mono = Mono.create(emitter -> {
            OkHttpClientCallback clientCallback = clientCallbackFactory.createCallback(feedRequest, emitter);
            enqueue(feedRequest, clientCallback);
        });
        return mono.publishOn(Schedulers.parallel()) // emitted results need to be published on parallel scheduler so we do not execute pulled data processing on the httpClient's own threadpool
                .toFuture();
    }

    private void enqueue(FeedRequest feedRequest, OkHttpClientCallback responseCallback) {
        Call call = createCall(feedRequest);
        call.enqueue(responseCallback);
        logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).descriptor("http_client_got_accepted_rq")).info("HttpClient accepted new request: {}", feedRequest.toStringBasicInfo());
    }

    private Call createCall(FeedRequest feedRequest) {
        okhttp3.OkHttpClient okHttpClient = clientsByPriorityMap.get(feedRequest.getPriority());
        Request.Builder builder = new Request.Builder()
                .get()
                .url(feedRequest.getUrl());

        for (FeedRequestHttpHeader httpHeader : feedRequest.getHttpHeaders()) {
            builder.addHeader(httpHeader.getKey(), httpHeader.getValue());
        }

        Request request = builder.build();

        return okHttpClient.newCall(request);
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down {}", getClass().getSimpleName());
        for (ExecutorService clientExecutorService : clientExecutorServices) {
            clientExecutorService.shutdown();
        }
    }

}
