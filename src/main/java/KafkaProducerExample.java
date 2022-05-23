import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.Instant.now;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
     static KafkaProducerConfig config;
     static KafkaProducer<String, Customer> producer;
     static Random rnd;

     static int eventsPerSeconds;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        rnd = new Random();
        config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        producer = new KafkaProducer<String, Customer>(props);
        log.info("Sending {} messages ...", config.getMessageCount());

        // over all the workload

       // fiveEpsToeachPartitonForFiveSeconds();
        //P1P216EPSOthers5EPSFor1min();

        //tenEventsPerSecForThreeMinute();


        //sleep for one 1 minute to avoid start sending during rebalancing
/*        Thread.sleep(1000*60);

         fiveEpsToeachPartitonForFiveSeconds();
        P1P216EPSOthers5EPSFor1min();*/

       /* increaseExponentially();
        remainConstantFor1Min();*/

        //OldWorkload.startWorkload();

        fiveEpsToeachPartitonForFiveSeconds();

        P1P216EPSOthers5EPSFor1min();
        fiveEpsToeachPartitonForFiveSeconds();
       // IncreaseDecreaseLinearly.startWorkload();


        //twentyFiveEventsPerSecForThreeMinute();


        //////////////////////////////////////////////////////



        //tenEventsPerSecForOneMinute();
        //tenEventsPerSecForOneMinute();
        //TwentyEventsPerSecForOneMinute();
        //TwentyEventsPerSecForOneMinute();
       // ThirtyEventsPerSecForOneMinute();
        //ThirtyEventsPerSecForOneMinute();

       /* FourteeEventsPerSecForOneMinute();
        increase1EventPerSecFor1min();
        remainConstantFor1min();
        increase1EventPerSecFor2min();
        remainConstant();*/
    }

    static void tenEventsPerSecForThreeMinute() throws InterruptedException {
        eventsPerSeconds = 10;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 2) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 10 events per sec for One Minute");
        log.info("==========================================");
    }

    static void twentyFiveEventsPerSecForThreeMinute() throws InterruptedException {
        eventsPerSeconds = 25;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 2) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 10 events per sec for One Minute");
        log.info("==========================================");
    }




    static void tenEventsPerSecForOneMinute() throws InterruptedException {
        eventsPerSeconds = 10;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 10 events per sec for One Minute");
        log.info("==========================================");
    }


    static void TwentyEventsPerSecForOneMinute() throws InterruptedException {
        eventsPerSeconds = 20;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }
    }

    static void ThirtyEventsPerSecForOneMinute() throws InterruptedException {
        eventsPerSeconds = 30;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }
    }

    static void FourteeEventsPerSecForOneMinute() throws InterruptedException {
         eventsPerSeconds = 40;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 30 events per sec for One Minute");
        log.info("==========================================");

    }

    static void increase1EventPerSecFor1min() throws InterruptedException {
        eventsPerSeconds = 41;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);
            eventsPerSeconds++;
            end = now();
        }
        log.info("End sending increase linearly  for One Minute");
        log.info("==========================================");
    }


    static void remainConstantFor1min() throws InterruptedException {
        log.info("From now on I am remaining constant with {} events per sec", eventsPerSeconds);

        Instant start = now();
        Instant end = now();

        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                // log.info("Sending the following customer {}",  custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for 1 second ");
            Thread.sleep(1000);
            end = now();
        }
    }


    static void increase1EventPerSecFor2min() throws InterruptedException {
        eventsPerSeconds = 100;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 1) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);
            eventsPerSeconds++;
            end = now();
        }
        log.info("End sending increase linearly  for One Minute");
        log.info("==========================================");
    }

    static void remainConstant() throws InterruptedException {
        log.info("From now on I am remaining constant with {} events per sec", eventsPerSeconds);


        while (true) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                // log.info("Sending the following customer {}",  custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for 1 second ");
            Thread.sleep(1000);
        }

    }


    static void fiveEpsToeachPartitonForFiveSeconds() throws InterruptedException {
        log.info("I will send five events per seconds for each partition for a " +
                "duration of 1 minute ");


        eventsPerSeconds = 100;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 1) {
            for (int j = 0; j < 5; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent 5 events per sec to each partition");
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);

            end = now();
        }
        log.info("End five events per seconds for each partition ");
        log.info("==========================================");

    }


    static void P1P216EPSOthers5EPSFor1min() throws InterruptedException {
        log.info("I will send five events per seconds for each partition for a " +
                "duration of 1 minute ");


        eventsPerSeconds = 100;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 1) {
            Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());

            for (int j = 0; j < 16; j++) {
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
            }

            for (int j = 0; j < 5; j++) {
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent 16 P1 P2 and 5 Otherwise  events per sec to each partition");
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);

            end = now();
        }
        log.info("End sent 16 P1 P2 and 5 Otherwise  events per sec to each partition ");
        log.info("==========================================");

    }




    static void increaseExponentially() throws InterruptedException {
        log.info("I will increase exponentially starting from 1 minute ");


        eventsPerSeconds = 0;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < Math.ceil(Math.pow(eventsPerSeconds, 2.0)); j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));

            }
            eventsPerSeconds++;
            log.info("sent  Math.exp(eventsPerSeconds) {} {}", eventsPerSeconds, Math.ceil(Math.exp(eventsPerSeconds)) );
            log.info("sleeping for one second ");
            Thread.sleep(1000);

            end = now();
        }
        log.info("End five events per seconds for each partition ");
        log.info("==========================================");

    }


    static void remainConstantFor1Min() throws InterruptedException {
        log.info("From now on I am remaining constant with {} events per sec", eventsPerSeconds);


        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                // log.info("Sending the following customer {}",  custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for 1 second ");
            Thread.sleep(1000);
            end = now();

        }

    }




}








