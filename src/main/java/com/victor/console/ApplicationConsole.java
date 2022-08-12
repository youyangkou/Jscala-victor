package com.victor.console;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Slf4j
@SpringBootApplication
@EnableScheduling
public class ApplicationConsole {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ApplicationConsole.class);
        application.run();
    }
}
