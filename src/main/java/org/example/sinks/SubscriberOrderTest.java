package org.example.sinks;

import org.junit.Test;
import reactor.core.publisher.Sinks;

public class SubscriberOrderTest {

    @Test
    public void consumeOrder(){
        Sinks.One<Integer> sinkOne = Sinks.one();

        sinkOne.asMono()
                .cache().flux()
                .next()
                .subscribe(data -> System.out.println("Subscribe 1 received: " + data));

        sinkOne.asMono()
                .cache().flux()
                .next()
                .subscribe(data -> System.out.println("Subscribe 2 received: " + data));

        sinkOne.asMono()
                .cache().flux()
                .next()
                .subscribe(data -> System.out.println("Subscribe 3 received: " + data));

        sinkOne.tryEmitValue(1);
    }
}
