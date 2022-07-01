package org.example.sinks;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

public class FuseableTest {
    private static final Logger logger = LoggerFactory.getLogger(FuseableTest.class);

    @Test
    public void testFlux() {
        Flux.just(1, 2, 3)
                .map(data -> data + 1)
                .subscribe(data -> logger.info("Data:" + data));
    }

    @Test
    public void testPublishOn() throws InterruptedException {
        Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("Request: " + request))
                .publishOn(Schedulers.boundedElastic(), 1)   //this.queue=FluxArraySubscription
                .subscribe(data -> logger.info("Data:" + data));

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testPublishOnFuseable() throws InterruptedException {
        Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("Request: " + request))
                .publishOn(Schedulers.boundedElastic(), 1) //this.queue=FluxArraySubscription
                .publishOn(Schedulers.boundedElastic(), 1)  //this.queue=PublishOnSubscriber (ASYNC: will request p when p == limit)
                .subscribe(data -> logger.info("Data:" + data));

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void testPublishOnWithMapFuseable() throws InterruptedException {
        Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("Request: " + request))
                .publishOn(Schedulers.boundedElastic(), 1) //this.queue=FluxArraySubscription
                .map(data -> data)
                .publishOn(Schedulers.boundedElastic(), 1)  //this.queue=PublishOnSubscriber (ASYNC: will request p when p == limit)
                .subscribe(data -> logger.info("Data:" + data));

        TimeUnit.SECONDS.sleep(1);
    }

    //Problem 1: requestMode = ASYNC, PublishOnSubscriber.poll() is called, so request new message before onNext() finished.
    @Test
    public void testPublishOnWithMerge() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source);

        Flux.merge(sourceSinks, 1).subscribe(data -> logger.info("Finished Process Data:" + data));
        ;

        TimeUnit.SECONDS.sleep(1);
    }

    //Solution 1
    @Test
    public void testSolvePublishOnWithMerge() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1)
                .map(message -> message);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source);

        Flux.merge(sourceSinks, 1).subscribe(data -> logger.info("Finished Process Data:" + data));
        ;

        TimeUnit.SECONDS.sleep(1);
    }

    //Problem 2: If tryEmit() failure(parent is wip), FluxFlatMap will also save data in queue
    @Test
    public void testMergeConcurrency2() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1)
                .map(message -> message);

        Flux<Integer> source2 = Flux.just(4, 5, 6)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1)
                .map(message -> message);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source2);

        Flux.merge(sourceSinks, 2).subscribe(data -> {
            logger.info("Finished Process Data:" + data);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });

        TimeUnit.SECONDS.sleep(5);
    }

    //Solution: wrap sync into publishOn
    @Test
    public void testSetPublishOnSync() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .flatMap(Flux::just, 1)
                .publishOn(Schedulers.boundedElastic(), 1)
                .flatMap(Flux::just, 1);

        Flux<Integer> source2 = Flux.just(4, 5, 6)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .flatMap(Flux::just, 1)
                .publishOn(Schedulers.boundedElastic(), 1)
                .flatMap(Flux::just, 1);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source2);

        Flux.merge(sourceSinks, 2).subscribe(data -> {
            logger.info("Finished Process Data:" + data);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });

        TimeUnit.SECONDS.sleep(5);
    }

    //Solution: wrap sync into publishOn
    @Test
    public void testWrapInToPublishOn() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data));

        Flux<Integer> source2 = Flux.just(4, 5, 6)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data));

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source2);

        Mono.fromRunnable(() -> {
            Flux.merge(sourceSinks, 2, 1).subscribe(data -> {
                logger.info("Finished Process Data:" + data);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }).subscribeOn(Schedulers.boundedElastic()).block();

        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void testParallelWithMerge() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1)
                .map(message -> message);

        Flux<Integer> source2 = Flux.just(4, 5, 6)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1)
                .map(message -> message);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source2);

        Flux.merge(sourceSinks, 2)
                .parallel(2)
                .runOn(Schedulers.boundedElastic(),1).subscribe(data -> {
            logger.info("Finished Process Data:" + data);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });

        TimeUnit.SECONDS.sleep(5);
    }

}
