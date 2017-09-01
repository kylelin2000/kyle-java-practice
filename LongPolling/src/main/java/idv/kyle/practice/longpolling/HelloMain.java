package idv.kyle.practice.longpolling;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableAutoConfiguration
@ComponentScan
public class HelloMain {
    public static void main(String[] args) {
	SpringApplication.run(HelloMain.class, args);
    }
}
