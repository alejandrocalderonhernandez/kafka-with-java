package com.alejandro.kafka.producer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alejandro.kafka.util.Constants;

public class MyProducer {

	Properties props;

	
	
	public MyProducer() {
		this.props = new Properties();
		this.loadProperties();
	}
	
	public void start(){
		try(Producer<String, String> producer = new KafkaProducer<>(this.props)) {
			IntStream stream = IntStream.range(0, 999);
			stream.forEach(i ->	producer.send(new ProducerRecord<String, String>(Constants.TOPIC_NAME, i+"", "some value")));
			producer.flush();
		} 
		
	}
	
	private void loadProperties() {
		try {
			this.props.load(new FileReader("src/main/resources/producer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
