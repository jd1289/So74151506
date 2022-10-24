package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@SpringBootApplication
public class So74151506Application {

	public static void main(String[] args) {
		SpringApplication.run(So74151506Application.class, args);
	}

	@KafkaListener(id = "so74151506", topics = "so74151506")
	void listen(List<String> in) {
		in.forEach(str->{
			System.out.println(str);
			if("bar".equalsIgnoreCase(str)) throw new NullPointerException();
		});
	}

	@Bean
	public NewTopic topic() {
		return TopicBuilder.name("so74151506").partitions(1).replicas(1).build();
	}

	@Bean
	ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> {
			template.send("so74151506", "test");
			template.send("so74151506", "foo");
			template.send("so74151506", "bar");
			template.send("so74151506", "cheese");
			template.send("so74151506", "herp");
		};
	}

	@Bean
	CommonErrorHandler eh() {
		DefaultErrorHandler eh = new DefaultErrorHandler((rec, ex) -> System.out.println("In recoverer"),
				new FixedBackOff(1000, 4));
		eh.addNotRetryableExceptions(NullPointerException.class);
		return eh;
	}
}
