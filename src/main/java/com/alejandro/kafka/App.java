package com.alejandro.kafka;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.alejandro.kafka.consumer.ConsumerMultiThread;

public class App {

	public static void main(String[] args) {
		Properties props = null;
		try {
		  props.load(new FileReader("src/main/resources/consumer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		for (int i = 0; i < 5; i++) {
			ConsumerMultiThread consumer = new ConsumerMultiThread(new KafkaConsumer<>(props));
			executor.execute(consumer);
		}
		while(!executor.isTerminated());
	}
}
