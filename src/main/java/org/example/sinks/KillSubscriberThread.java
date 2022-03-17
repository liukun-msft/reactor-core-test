package org.example.sinks;

import reactor.core.publisher.Sinks;

import java.util.concurrent.TimeUnit;

public class KillSubscriberThread {

    static class Source {
        Sinks.Many<Integer> downstream;

        public void generate(int data) {
            Sinks.EmitResult result = downstream.tryEmitNext(data);

            //We can use currentSubscriberCount() to know current subscriber on this sink
            if (result != Sinks.EmitResult.OK) {
                System.out.printf("[%s] emit failure, result [%s], current subscriber[%s] \n", Thread.currentThread().getName(), result, downstream.currentSubscriberCount());
            } else {
                System.out.printf("[%s] emit success, current subscriber[%s]\n", Thread.currentThread().getName(), downstream.currentSubscriberCount());
            }
        }

        public void setDownstream(Sinks.Many<Integer> downstream) {
            this.downstream = downstream;
        }
    }

    static class Receiver {
        public Iterable<Integer> receive(Source source) {
            Sinks.Many<Integer> emitter = Sinks.many().replay().all();
            source.setDownstream(emitter);
            return emitter.asFlux().toIterable();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Source source = new Source();
        Receiver receiver = new Receiver();
        Thread t = createThreadToReceive(source, receiver);
        t.start();


        //Wait downstream set to source
        TimeUnit.SECONDS.sleep(1);

        source.generate(1);
        source.generate(2);
        //Will kill receive thread
        source.generate(3);

        //Still could emit after receive thread killed
        //But the subscriber number change to 0
        TimeUnit.SECONDS.sleep(3);
        source.generate(4);
        source.generate(5);

    }

    private static Thread createThreadToReceive(Source source, Receiver receiver) {
        return new Thread(() -> {

            for (Integer i : receiver.receive(source)) {
                System.out.printf("[%s] Receive index: %d \n", Thread.currentThread().getName(), i);
                if (i == 3) {

                    //NOTE: Throw exception to kill the thread  won't unsubscribe current subscriber
                    throw new IllegalStateException("kill thread by exception");

                    //Interrupt thread will unsubscribe current subscriber
                    //Thread.currentThread().interrupt();
                }
            }
        });
    }

}
