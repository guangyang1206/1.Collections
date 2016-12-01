package com.epam.concurrency.task;

/**
 * Created by Tanmoy on 6/17/2016.
 */
public class Util {

    public static void sleepToSimulateDataHeavyProcessing(){
        sleepToSimulateDataHeavyProcessing(1000 * 1);
    }
    public static void sleepToSimulateDataHeavyProcessing(long timeInMs){
        try{
            Thread.sleep(timeInMs);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
