package org.example.sinks;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

public class ThrowErrorTest {

    private static final Logger logger = LoggerFactory.getLogger(ThrowErrorTest.class);

    @Test
    public void testPublishOn() throws InterruptedException {
        Flux.just(1, 2, 3, 4, 5)
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(data -> {
                    throw new RuntimeException();
                })
                .map(data -> data)
                .doOnError(err -> {
                    throw new RuntimeException();
                })
                .subscribe();

        TimeUnit.SECONDS.sleep(10);
    }
}
