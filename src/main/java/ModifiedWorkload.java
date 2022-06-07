import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import static java.time.Instant.now;

public class ModifiedWorkload {


    private Random rnd;
    int events = 0;

    private int eventsPerSeconds;

    public ModifiedWorkload() {
        rnd = new Random();
    }


    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);


    public void start() throws InterruptedException {

        fifteenEpsToeachPartitonForOneMinutes();
        fifteenEpsIncreaseLinearlyToeachPartitonForOneMinutes();
        P1P260EPSOthers15EPSForOneMinutes();
        P1P260EPSOthers15EPSForOneMinutesIncrease();
        P1P2P360EPSOthers15EPSForTwoMinutes();

    }


    private void fifteenEpsToeachPartitonForOneMinutes() throws InterruptedException {
        log.info("I will send 15  events per seconds for each partition for a " +
                "duration of 75 seconds ");


        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toSeconds() <= 75) {
            for (int j = 0; j < 15; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent 15 events per sec to each partition");
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);

            end = now();
        }
        log.info("End 15 events per seconds for each partition ");
        log.info("==========================================");

    }


    private void fifteenEpsIncreaseLinearlyToeachPartitonForOneMinutes() throws InterruptedException {
        log.info("I will send 15 to P2, P3, P4 and increase linearly  events per seconds for eP0, P1 " +
                "for 45 secs ");
        Instant start = now();
        Instant end = now();

        events = 15;

        while (Duration.between(start, end).toSeconds() <= 45) {
            for (int j = 0; j < events; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));

                //log.info("Sending the following customer {}", custm.toString());

            }


            for (int j = 0; j < 15; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());

                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent 15 to P2,3,4 and increase liearly to 60 for P1,2");
            log.info("sleeping for one seconds ");
            //events++;
            Thread.sleep(1000);

            end = now();
            events++;
        }
        log.info("End ");
        log.info("==========================================");

    }


    private void P1P260EPSOthers15EPSForOneMinutes() throws InterruptedException {
        log.info("I will send 60  for P0,P1 and 15 for others for 75 secs ");
        eventsPerSeconds = 15;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toSeconds() <= 75) {
            Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());

            for (int j = 0; j < events; j++) {
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
            }

            for (int j = 0; j < 15; j++) {
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            /*log.info("sent 90 EPS  P1 P2 and 15 Otherwise  events per sec to each partition");
            log.info("sleeping for one seconds ");*/
            Thread.sleep(1000);

            end = now();
        }
        log.info("End sent 16 P1 P2 and 5 Otherwise  events per sec to each partition ");
        log.info("==========================================");

    }


    private void P1P260EPSOthers15EPSForOneMinutesIncrease() throws InterruptedException {
        log.info("Increase linearly for P3 only ");


        eventsPerSeconds = 15;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toSeconds() <= 45) {
            Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());

            for (int j = 0; j < events; j++) {
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //events++;
            }


            for (int j = 0; j < eventsPerSeconds; j++) {
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));

            }

            for (int j = 0; j < 15; j++) {

                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }

            log.info("Sent 16 P1 P2 and 5 Otherwise  events per sec to each partition increase P2");

      /*      log.info("sent 90 EPS  P1 P2 and 15 Otherwise  events per sec to each partition");
            log.info("sleeping for one seconds ");*/

            Thread.sleep(1000);
            eventsPerSeconds++;

            end = now();
        }
        log.info("End sent 16 P1 P2 and 5 Otherwise  events per sec to each partition increase P2");
        log.info("==========================================");

    }


    private void P1P2P360EPSOthers15EPSForTwoMinutes() throws InterruptedException {
        log.info("I will send five events per seconds for each partition for a " +
                "duration of 1 minute ");


        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 1) {
            Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());

            for (int j = 0; j < 60; j++) {
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        0, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        1, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        2, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
            }

            for (int j = 0; j < 15; j++) {

                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        3, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                KafkaProducerExample.producer.send(new ProducerRecord<String, Customer>(KafkaProducerExample.config.getTopic(),
                        4, System.currentTimeMillis(), UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent 90 EPS  P1 P2 and 15 Otherwise  events per sec to each partition");
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);

            end = now();
        }
        log.info("End sent 16 P1 P2 and 5 Otherwise  events per sec to each partition ");
        log.info("==========================================");

    }


}
