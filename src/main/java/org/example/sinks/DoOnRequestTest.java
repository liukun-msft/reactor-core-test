package org.example.sinks;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;
import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;

public class DoOnRequestTest {

    private static final Logger logger = LoggerFactory.getLogger(DoOnRequestTest.class);

    private Subscriber subscriber = new CoreSubscriber<String>() {
        private Subscription subscription = null;

        @Override
        public void onSubscribe(Subscription subscription) {
            logger.info("Subscriber - onSubscribe called");
            this.subscription = subscription;
            logger.info("Subscriber - Requesting 1 more message onSubscribe");
            subscription.request(1);
        }

        @Override
        public void onNext(String message) {
            logger.info("Subscriber - onNext called");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Subscriber - Process finished: " + message);
            logger.info("Subscriber - Requesting 1 more message onNext");
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onComplete() {
        }
    };

    @Test
    public void testDoOnRequest() throws InterruptedException {
        Scheduler scheduler = Schedulers.newBoundedElastic(3, 3, "receiver-");

        Sinks.Many<String> sinks = Sinks.many().replay().all();
        Flux<String> flux1 = sinks.asFlux()
                .doOnSubscribe(subscription -> {
                    logger.info("Flux1 - doOnSubscribe called");
                })
                .doOnRequest(request -> {
                    //request was impact by prefetch number
                    logger.info("Flux1 - doOnRequest called, request number: " + request);
                })
                .doOnNext(message -> {
                    logger.info("Flux1 - doOnNext called, received message : " + message);
                })
                // When prefetch is 1, will set request to be 1
                // When prefetch is 2, then received 2 messages although request 1
                // If not set, will receive 3 message and consume one by one
                .publishOn(scheduler, 1)
                .log();

        flux1.subscribe(subscriber);

        sinks.emitNext("First Data", FAIL_FAST);
        sinks.emitNext("Second Data", FAIL_FAST);
        sinks.emitNext("Third Data", FAIL_FAST);


        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void testFlatMany() throws InterruptedException {
        Scheduler scheduler = Schedulers.newBoundedElastic(5, 5, "receiver-");

        Sinks.Many<String> sinks = Sinks.many().replay().all();
        Flux<String> flux1 = sinks.asFlux()
                .publishOn(Schedulers.boundedElastic())
                .publishOn(scheduler)
                .doOnSubscribe(subscription -> {
                    logger.info("Flux1 - doOnSubscribe called");
                })
                .doOnRequest(request -> {
                    logger.info("Flux1 - doOnRequest called, request number: " + request);
                })
                .doOnNext(message -> {
                    logger.info("Flux1 - doOnNext called, received message : " + message);
                });
//                .log();

        Flux<String> flux2 = Mono.just("Active link 1")
                .flatMapMany(link -> flux1)
                .publishOn(scheduler, 1);

        flux2.subscribeOn(scheduler).subscribe(subscriber);

        sinks.emitNext("First Data", FAIL_FAST);
        sinks.emitNext("Second Data", FAIL_FAST);
        sinks.emitNext("Third Data", FAIL_FAST);


        TimeUnit.SECONDS.sleep(5);
    }


    @Test
    public void testEmitterProcessor() throws InterruptedException {
        Scheduler scheduler = Schedulers.newBoundedElastic(5, 5, "receiver-");
        Sinks.Many<String> sinks = Sinks.many().replay().all();

        Flux<String> flux1 = sinks.asFlux()
                .publishOn(Schedulers.boundedElastic())
                .publishOn(scheduler)
                .doOnSubscribe(subscription -> {
                    logger.info("Flux1 - doOnSubscribe called");
                })
                .doOnRequest(request -> {
                    logger.info("Flux1 - doOnRequest called, request number: " + request);
                })
                .doOnNext(message -> {
                    logger.info("Flux1 - doOnNext called, received message : " + message);
                })
//                .publishOn(scheduler, 1)
                .log();


        Flux<String> flux2 = Mono.defer(() -> Mono.just("active link 1"))
                .flatMapMany(link -> flux1.doFinally(message -> logger.info("finally")));
//                .publishOn(scheduler, 1);

        EmitterProcessor<Flux<String>> processor = EmitterProcessor.create(5, false);
        FluxSink<Flux<String>> sessionReceiveSink;
        sessionReceiveSink = processor.sink();
        sessionReceiveSink.next(flux2);

        Flux<String> merged = Flux.merge(processor,1).log();

        merged.subscribe(subscriber);

        new Thread(() -> {
            logger.info("-------------- emit data ---------");
            sinks.emitNext("First Data", FAIL_FAST);
            sinks.emitNext("Second Data", FAIL_FAST);
            sinks.emitNext("Third Data", FAIL_FAST);
        }).start();

        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void testSinks() throws InterruptedException {
        Scheduler scheduler = Schedulers.newBoundedElastic(2, 2, "receiver-");
        Sinks.Many<String> sinks = Sinks.many().replay().all();

        Flux<String> flux1 = sinks.asFlux()
                .publishOn(Schedulers.boundedElastic())
                .publishOn(scheduler)
                .doOnSubscribe(subscription -> {
                    logger.info("Flux1 - doOnSubscribe called");
                })
                .doOnRequest(request -> {
                    logger.info("Flux1 - doOnRequest called, request number: " + request);
                })
                .doOnNext(message -> {
                    logger.info("Flux1 - doOnNext called, received message : " + message);
                });


        Flux<String> flux2 = Mono.defer(() -> Mono.just("Active link 1"))
                .flatMapMany(link -> flux1)
                .publishOn(scheduler,1);
//                .log();
//                .map(message -> message);

        Sinks.Many<Flux<String>> processor = Sinks.many().replay().all();
        processor.tryEmitNext(flux2);

        Flux<String> merged = Flux.merge(processor.asFlux(),2).log();

        merged.subscribe(subscriber);

        new Thread(() -> {
            logger.info("-------------- emit data ---------");
            sinks.emitNext("First Data", FAIL_FAST);
            sinks.emitNext("Second Data", FAIL_FAST);
            sinks.emitNext("Third Data", FAIL_FAST);
        }).start();

        TimeUnit.SECONDS.sleep(5);
    }


}
