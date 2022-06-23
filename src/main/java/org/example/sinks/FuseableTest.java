package org.example.sinks;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

public class FuseableTest {
    private static final Logger logger = LoggerFactory.getLogger(FuseableTest.class);

    @Test
    public void testFlux(){
        Flux.just(1, 2, 3)
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

    //Problem 1: requestMode = ASYNC, PublishOnSubscriber.poll() is called, so request new message before onNext() finished.
    @Test
    public void testPublishOnWithMerge() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source);

        Flux.merge(sourceSinks, 1).subscribe(data -> logger.info("Finished Process Data:" + data));;

        TimeUnit.SECONDS.sleep(1);
    }

    //Solution 1
    @Test
    public void testSolvePublishOnWithMerge() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source);

        Flux.merge(sourceSinks, 1).subscribe(data -> logger.info("Finished Process Data:" + data));;

        TimeUnit.SECONDS.sleep(1);
    }

    //Problem 2: If tryEmit() failure(parent is wip), FluxFlatMap will also save data in queue
    @Test
    public void testMergeConcurrency2() throws InterruptedException {
        Flux<Integer> source = Flux.just(1, 2, 3)
                .doOnRequest(request -> logger.info("--- Request: " + request))
                .doOnNext(data -> logger.info("send data: " + data))
                .publishOn(Schedulers.boundedElastic(), 1);

        Flux<Flux<Integer>> sourceSinks = Flux.just(source, source);

        Flux.merge(sourceSinks, 2).subscribe(data -> logger.info("Finished Process Data:" + data));;

        TimeUnit.SECONDS.sleep(1);
    }

}
