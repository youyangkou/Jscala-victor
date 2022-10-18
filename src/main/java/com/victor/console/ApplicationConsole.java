package com.victor.console;

import com.victor.console.event.MyAppEvent;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author Gerry
 * @date 2022-08-12
 */
@Slf4j
@Configuration
@SpringBootApplication(scanBasePackages = "com.victor.console")
@EnableScheduling
@MapperScan("com.victor.console.dao")
public class ApplicationConsole {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ApplicationConsole.class);
        ConfigurableApplicationContext applicationContext = application.run(ApplicationConsole.class, args);
        int[] array = {1, 2, 3, 4};
        applicationContext.publishEvent(new MyAppEvent(array));

    }
}
