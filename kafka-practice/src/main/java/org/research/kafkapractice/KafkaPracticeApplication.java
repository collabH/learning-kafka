package org.research.kafkapractice;

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

        partitionerProducer.sendPartitioner("test Last Partitioner","test-topic");
        partitionerProducer.sendPartitioner("test Last Partitioner","test-topic");
    }

}
