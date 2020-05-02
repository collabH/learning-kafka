package org.research.kafkapractice;

import org.research.kafkapractice.consumer.OriginalConsumer;
import org.research.kafkapractice.consumer.ShutDownHookConsumer;
import org.research.kafkapractice.consumer.StandAloneConsumer;
import org.research.kafkapractice.parititioner.PartitionerProducer;
import org.research.kafkapractice.producer.MultiProducer;
import org.research.kafkapractice.producer.OriginalProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafkaPracticeApplication implements CommandLineRunner {
    @Autowired
    private MultiProducer multiProducer;

    @Autowired
    private OriginalProducer originalProducer;

    @Autowired
    private PartitionerProducer partitionerProducer;

    @Autowired
    private OriginalConsumer originalConsumer;

    @Autowired
    private ShutDownHookConsumer shutDownHookConsumer;

    @Autowired
    private StandAloneConsumer standAloneConsumer;

    public static void main(String[] args) {
        SpringApplication.run(KafkaPracticeApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

//        multiProducer.send1();
//        multiProducer.send2();
//        multiProducer.send3();

//        originalProducer.syncSendMsg("hello world orignal", "test-topic");
//        originalProducer.asyncSendMsg("async send msg", "test-topic");

//        originalProducer.syncSendMsg("test Last Partitioner", "learning-kafka");


        //  originalConsumer.consumer();
//        originalConsumer.concurrentConsumer();
//        originalConsumer.asyncCommitOffsetConsumer();
//        originalConsumer.combinationCommitOffset();

//        originalConsumer.reblaceListenerConsumer();
//        shutDownHookConsumer.shutDownConsumer();
        standAloneConsumer.consumer();
    }


}
