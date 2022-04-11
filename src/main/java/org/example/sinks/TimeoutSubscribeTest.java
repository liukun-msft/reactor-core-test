package org.example.sinks;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeoutSubscribeTest {

    @Test
    public void testTimeout() throws InterruptedException {
        Sinks.One<Void> closeMono = Sinks.one();

        Mono<Void> timeout = closeMono.asMono()
                .timeout(Duration.ofSeconds(3))
                .onErrorResume(TimeoutException.class,
                        err -> Mono.fromRunnable(() -> System.out.println(err)));


//       timeout.subscribe(data -> System.out.println(data));

//        Mono.whenDelayError(timeout.doFinally(signalType -> { System.out.println("test");})).then(Mono.fromRunnable(() -> {
//            System.out.println("Receive mono");
//        })).subscribe();


//        timeout.subscribe();

//        timeout.then(Mono.fromRunnable(() -> {
//            System.out.println("Receive mono");
//        })).subscribe();

        Mono.whenDelayError(timeout).then(Mono.fromRunnable(() -> {
            System.out.println("Receive mono");
        })).subscribe();

        TimeUnit.SECONDS.sleep(2);

        closeMono.tryEmitEmpty();

        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void flux() {
        Flux<Integer> flux = Flux.just(1, 2, 3);

        flux.subscribe(data -> System.out.println(data));
    }


}
