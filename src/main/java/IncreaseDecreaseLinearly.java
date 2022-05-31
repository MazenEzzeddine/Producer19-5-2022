import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import static java.time.Instant.now;

public class IncreaseDecreaseLinearly {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);

    private static int eventsPerSeconds;
    private static  Random rnd;

    public IncreaseDecreaseLinearly(int eventsPerSeconds) {
        IncreaseDecreaseLinearly.eventsPerSeconds = eventsPerSeconds;
    }


    public IncreaseDecreaseLinearly() {

    }



    public static void startWorkload() throws InterruptedException {
        TenFor1min();
        increaseLinearlyFor1Min();
        remainConstantFor1min();
        decreaseLinearlyFor1Min();
        remainConstantFor1min();


    }

    private static void TenFor1min() throws InterruptedException {

        rnd = new Random();

        eventsPerSeconds = 10;
        Instant start = now();
        Instant end = now();
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("@ eventsPerSeconds, constant  for 1 min {}", eventsPerSeconds);

        while (Duration.between(start, end).toMinutes() <= 1) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        null, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 10 events per sec for One Minute");
        log.info("eventsPerSeconds {}", eventsPerSeconds);

        log.info("==========================================");
    }



    private static void increaseLinearlyFor1Min() throws InterruptedException {
        Instant start = now();
        Instant end = now();
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("@ eventsPerSeconds, increase linearly  for 1 min {}", eventsPerSeconds);
        while (Duration.between(start, end).toMinutes() <= 0) {
            eventsPerSeconds ++;
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        null, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }
        log.info("End increaseLinearlyFor1Min ");
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("==========================================");
    }

    private static void remainConstantFor1min() throws InterruptedException {
        Instant start = now();
        Instant end = now();
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("@ eventsPerSeconds, constant  for 1 min {}", eventsPerSeconds);
        while (Duration.between(start, end).toMinutes() <= 1) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        null, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End @ eventsPerSeconds, constant  for 1 min ");
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("==========================================");
    }

    private static  void decreaseLinearlyFor1Min() throws InterruptedException {
        Instant start = now();
        Instant end = now();
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("@ eventsPerSeconds, increase linearly  for 1 min {}", eventsPerSeconds);
        while (Duration.between(start, end).toMinutes() <= 0) {
            eventsPerSeconds --;
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        null, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End @ eventsPerSeconds, decrease linealry  for 1 min ");
        log.info("eventsPerSeconds {}", eventsPerSeconds);
        log.info("==========================================");
    }






}
