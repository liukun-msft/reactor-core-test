package org.example.sinks;


import org.junit.Test;
import reactor.core.publisher.Sinks;

import java.util.concurrent.TimeUnit;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class KillThreadTest {

    @Test
    public void testKillThreadUseToIterate() throws InterruptedException {
        Sinks.Many<Integer> sinks = Sinks.many().replay().all();
        Thread t = createThreadToIterate(sinks);
        t.start();

        //Wait subscribed
        TimeUnit.SECONDS.sleep(1);
        System.out.printf("[%s] Current subscriber count: %d \n",
                Thread.currentThread().getName(), sinks.currentSubscriberCount());

        sinks.emitNext(1, FAIL_FAST);
        sinks.emitNext(2, FAIL_FAST);
        sinks.emitNext(3, FAIL_FAST);

        //Wait Thread killed
        TimeUnit.SECONDS.sleep(2);

        //When use toIterable(), the subscriber count is 1, but no data received since thread is killed.
        System.out.printf("[%s] Current subscriber count: %d \n",
                Thread.currentThread().getName(), sinks.currentSubscriberCount());
        sinks.emitNext(4, FAIL_FAST);
        sinks.emitNext(5, FAIL_FAST);
    }

    private static Thread createThreadToIterate(Sinks.Many<Integer> sinks) {
        return new Thread(() -> {
            for (Integer data : sinks.asFlux().toIterable()) {
                System.out.printf("[%s] Receive index: %d \n", Thread.currentThread().getName(), data);
                if (data == 3) {
                    throw new IllegalStateException("kill thread by exception");
                }
            }
        });
    }

    @Test
    public void testKillThreadUseSubscribe() throws InterruptedException {
        Sinks.Many<Integer> sinks = Sinks.many().replay().all();
        Thread t = createThreadToSubscribe(sinks);
        t.start();

        //Wait subscribed
        TimeUnit.SECONDS.sleep(1);
        System.out.printf("[%s] Current subscriber count: %d \n",
                Thread.currentThread().getName(), sinks.currentSubscriberCount());
        sinks.emitNext(1, FAIL_FAST);
        sinks.emitNext(2, FAIL_FAST);
        sinks.emitNext(3, FAIL_FAST);

        //Wait Thread killed
        TimeUnit.SECONDS.sleep(2);

        //When use subscribe(), the subscriber count is 0
        System.out.printf("[%s] Current subscriber count: %d \n",
                Thread.currentThread().getName(), sinks.currentSubscriberCount());
        sinks.emitNext(4, FAIL_FAST);
        sinks.emitNext(5, FAIL_FAST);
    }

    private static Thread createThreadToSubscribe(Sinks.Many<Integer> sinks) {
        return new Thread(() -> {
            sinks.asFlux().subscribe(data -> {
                System.out.printf("[%s] Receive index: %d \n", Thread.currentThread().getName(), data);
                if (data == 3) {
                    throw new IllegalStateException("kill current thread");
                }
            });
        });
    }


}
