package KafkaFeeder;

import org.apache.kafka.common.header.Header;

public class RecordHeader implements Header {
    final private int recordSize;
    
    public RecordHeader(int recordSize){
        this.recordSize = recordSize;
    }

    @Override
    public String key(){
        return "";
    }

    @Override
    public byte[] value(){
        return new byte[recordSize];
    }
}