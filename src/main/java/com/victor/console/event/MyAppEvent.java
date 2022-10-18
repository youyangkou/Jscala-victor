package com.victor.console.event;

import org.springframework.context.ApplicationEvent;

/**
 * @author Gerry
 * @date 2022-10-18
 */
public class MyAppEvent extends ApplicationEvent {

    String msg;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public MyAppEvent(Object source, String msg) {
        super(source);
        setMsg(msg);
        System.out.println("ApplicationEvent has been created");
    }
}
