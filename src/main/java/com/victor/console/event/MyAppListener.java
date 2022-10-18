package com.victor.console.event;

import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * @author Gerry
 * @date 2022-10-18
 */
@Component
public class MyAppListener implements ApplicationListener<MyAppEvent> {

    @Override
    public void onApplicationEvent(MyAppEvent event) {
        int[] source = (int[]) event.getSource();
        System.out.println("listener has get the event,source=:" + Arrays.toString(source));
    }
}
