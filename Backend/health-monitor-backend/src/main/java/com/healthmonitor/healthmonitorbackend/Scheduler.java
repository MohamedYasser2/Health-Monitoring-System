package com.healthmonitor.healthmonitorbackend;

import org.testng.annotations.Test;

import java.util.Timer;
import java.util.TimerTask;

public class Scheduler {
    @Test
    public void givenUsingTimer_whenSchedulingDailyTask_thenCorrect() {
        TimerTask repeatedTask = new TimerTask() {
            public void run() {

            }
        };
        Timer timer = new Timer("Timer");

        long delay = 1000L;
        long period = 1000L * 60L * 60L * 24L;
        timer.scheduleAtFixedRate(repeatedTask, delay, period);
    }
}
