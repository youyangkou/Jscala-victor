package com.victor.console.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @author Gerry
 * @date 2022-10-18
 */
@Component
public class DemoPublisher {
    @Autowired
    ApplicationContext applicationContext;

    public void publish(String msg) {
        int[] array = {1, 2, 3, 4};
        applicationContext.publishEvent(new MyAppEvent(array, msg));
    }
}
