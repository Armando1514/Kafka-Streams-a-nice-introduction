package com.github.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BankTransactionsProducerTest {

    @Test
    public void newRandomTransactionsTest(){
        ProducerRecord<String, String> record = new BankTransactionsProducer().newRandomTransaction("john");
        String key = record.key();

        assertEquals(key, "john");
    }
}
