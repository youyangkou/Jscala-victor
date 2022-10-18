package com.victor.console.event;

import org.springframework.context.ApplicationEvent;

/**
 * @author Gerry
 * @date 2022-10-18
 */
public class MyAppEvent extends ApplicationEvent {

    public MyAppEvent(Object source) {
        super(source);
        System.out.println("ApplicationEvent has been created");
    }
}
