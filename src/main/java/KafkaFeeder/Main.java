package KafkaFeeder;
// std lib
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

// 3rd lib
import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Header;


import KafkaFeeder.RecordHeader;


public class Main {
    // define params from cli
    @Parameter(names={"--brokers"}) String brokers;
    @Parameter(names={"--topic"}) String topic;
    @Parameter(names={"--records"}) int records;
    @Parameter(names={"--recordSize"}) int recordSize;

    // define attributs
    int generatedRecordNum = 0;

    public static void main(String[] args) {
        Main main = new Main();
        main.parseArgs(main, args);
        main.showArgs();
        main.run();
    }

    private void parseArgs(Main main, String[] args){
        JCommander.newBuilder()
            .addObject(main)
            .build()
            .parse(args);
    }

    public void showArgs() {
        Utils.showNSymbol("#", 30);
        System.out.printf("brokers: %s\ntopic: %s\nrecords: %d\nrecordSize: %d\n", brokers, topic, records, recordSize);
        Utils.showNSymbol("#", 30);
    }

    private ProducerRecord<String, String> getRecord(){
        int index = generatedRecordNum++;
        List<Header> headers = new ArrayList<Header>();
        headers.add(new RecordHeader(recordSize));
        return new ProducerRecord(
            topic,
            null,
            String.format("key-%010d", index),
            String.format("value-%010d", index),
            headers
        );
    }

    public void run(){
        StringSerializer serializer = new StringSerializer();
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop, serializer, serializer);

        System.out.println("Start feeding.");
        while(generatedRecordNum < records){
            producer.send(getRecord());
        }
        producer.close();
        System.out.println("Finish feeding.");
    }
}