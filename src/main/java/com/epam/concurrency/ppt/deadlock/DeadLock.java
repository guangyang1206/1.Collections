package com.epam.concurrency.ppt.deadlock;

/**
 * Created by Tanmoy on 11/23/2016.
 */
public class DeadLock {
    private Object lock1 = new Object();
    private Object lock2 = new Object();

    public void methodA() throws InterruptedException {
        synchronized (lock1){
            Thread.sleep(1000);
            synchronized (lock2){
                //do something
            }
        }
    }

    public void methodB() throws InterruptedException {
        synchronized (lock2){
            Thread.sleep(1000);
            synchronized (lock1){
                //do something
            }
        }
    }
}
