package org.example.sinks;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposables;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

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

        flux2.subscribe(subscriber);

        sinks.emitNext("First Data", FAIL_FAST);
        sinks.emitNext("Second Data", FAIL_FAST);
        sinks.emitNext("Third Data", FAIL_FAST);


        TimeUnit.SECONDS.sleep(5);
    }


    @Test
    public void testWithConcat() throws InterruptedException {
        Scheduler scheduler = Schedulers.newBoundedElastic(5, 5, "receiver-");

        MonoProcessor<String> cancelReceiveProcessor = MonoProcessor.create();
        DirectProcessor<String> messageReceivedEmitter = DirectProcessor.create();
        FluxSink<String> messageReceivedSink = messageReceivedEmitter.sink(FluxSink.OverflowStrategy.BUFFER);

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
                .takeUntilOther(cancelReceiveProcessor)
                .doOnNext(message -> {
                    logger.info("Flux1 - doOnNext called, received message : " + message);

                    messageReceivedSink.next("token");
                }).publishOn(scheduler, 1);
//                .log();

        Flux<String> merged = Flux.concat(flux1, cancelReceiveProcessor);

        Flux.switchOnNext(messageReceivedEmitter
                        .map((String lockToken) -> Mono.delay(Duration.ofSeconds(2))))
                .subscribe(item -> {
                    cancelReceiveProcessor.onComplete();
                });

        Sinks.One<String> mono = Sinks.one();

        Flux<String> flux2 = mono.asMono()
                .flatMapMany(link -> merged)
                .publishOn(scheduler, 1);

        flux2.subscribe(subscriber);

        mono.tryEmitValue("Active link 1");

        sinks.emitNext("First Data", FAIL_FAST);
        sinks.emitNext("Second Data", FAIL_FAST);
        sinks.emitNext("Third Data", FAIL_FAST);


        TimeUnit.SECONDS.sleep(5);
    }


}
