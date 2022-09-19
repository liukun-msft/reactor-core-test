package org.example.sinks;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

public class ParallelTest {

    private static final Logger logger = LoggerFactory.getLogger(ParallelTest.class);

    // Problem: internally prefetch one more message
    @Test
    public void testParallelPrefetch() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .parallel(2, 1)
                .runOn(Schedulers.boundedElastic(), 1)
                .subscribe(data -> logger.info("Finished Process Data:" + data));

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testPublishOnDifferent() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 2) // will wait 2 messages finished then request next
                .subscribe(data -> {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Finished Process Data:" + data);
                });

        TimeUnit.SECONDS.sleep(10);
    }


    // Fix
    @Test
    public void testParallelPrefetchFix() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5, 6, 7)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .map(data -> data)
                .parallel(2, 1)
                .map(data -> data)
                .log()
                .runOn(Schedulers.boundedElastic(), 2)
                .subscribe(data -> {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Finished Process Data:" + data);
                });

        TimeUnit.SECONDS.sleep(10);
    }


    @Test
    public void testPublishOnPrefetch() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .limitRate(2)
                .publishOn(Schedulers.boundedElastic(), 1)
                .subscribe(data -> logger.info("Finished Process Data:" + data));

        TimeUnit.SECONDS.sleep(1);
    }
}
