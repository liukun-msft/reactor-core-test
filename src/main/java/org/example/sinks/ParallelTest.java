package org.example.sinks;

import org.junit.Test;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

public class ParallelTest {

    private static final Logger logger = LoggerFactory.getLogger(ParallelTest.class);

    // Problem 1: internally prefetch one more message
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

    // Problem 2: internally prefetch two more message
    @Test
    public void testParallelPrefetchWithLimitRate() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5, 6, 7)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                /*  Add limitRate */
                .limitRate(1)
                .map(data -> data) // prefetch one message if we comment this line out

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


    // Fix 1: use flatmap to replace parallel and remove limitRate
    @Test
    public void testFlatMap() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5, 6, 7)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                //.limitRate(1)
                .flatMap(data -> Mono.fromRunnable(() -> {
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            logger.info("Finished Process Data:" + data);
                        })
                        .publishOn(Schedulers.boundedElastic()), 2)
                .log()
                .subscribe();

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
}
