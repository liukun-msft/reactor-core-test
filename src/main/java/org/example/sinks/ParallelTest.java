package org.example.sinks;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class ParallelTest {

    private static final Logger logger = LoggerFactory.getLogger(ParallelTest.class);

    // Problem: internally prefetch one more message
    @Test
    public void testParallelPrefetch() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5, 6, 7)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .parallel(2, 1)
                .log()
                .runOn(Schedulers.boundedElastic(), 1)
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
    public void testParallelWithSubscriber() throws InterruptedException {
        CoreSubscriber<Integer>[] subscribers = new CoreSubscriber[2];

        for (int i = 0; i < 2; i++) {
            subscribers[i] = new CoreSubscriber<>() {
                private Subscription subscription = null;

                @Override
                public void onSubscribe(Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(1);
                }

                @Override
                public void onNext(Integer data) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Finished Process Data:" + data);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.info("Error receiving.", throwable);
                }

                @Override
                public void onComplete() {
                    logger.info("Completed receiving.");
                }
            };
        }

        Flux.just(1, 2, 3, 4, 5, 6, 7, 8)
                .doOnRequest(request -> {
                    logger.info("--- Request: " + request);
                })
                .doOnNext(data -> logger.info("send data: " + data))
                .limitRate(1)
                .parallel(2, 1)
                .log()
                .runOn(Schedulers.boundedElastic(), 1)
                .subscribe(subscribers);

        TimeUnit.SECONDS.sleep(10);
    }

    private class LinkProcessor extends FluxProcessor<String, Integer>
            implements Subscription {

        private Subscription upstream = null;

        @Override
        public void onSubscribe(Subscription subscription) {
            this.upstream = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(String link) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Create a new link" + link);
            upstream.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            logger.info("Error receiving.", throwable);
        }

        @Override
        public void onComplete() {
            logger.info("Completed receiving.");
        }

        @Override
        public void request(long l) {
            
        }

        @Override
        public void cancel() {

        }

        @Override
        public void subscribe(CoreSubscriber<? super Integer> downstream) {
            downstream.onSubscribe(this);
        }
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
                .limitRate(1)
                .parallel(2, 1)
                .log()
                .runOn(Schedulers.boundedElastic(), 2)
                .subscribe(data -> {
                    try {
                        TimeUnit.SECONDS.sleep(2);
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
