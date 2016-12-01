package com.epam.concurrency.ppt.stackerror;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Tanmoy on 11/23/2016.
 */
public class JvmStackCrash {

    static long n;


    public static void main(String[] a) throws IOException {
        heapCheck();
        n = 0;
        try {
            sub();
        } catch (StackOverflowError e) {
            e.printStackTrace();
            System.out.println("Maximum nested calls: "+n);
            heapCheck();
        }
    }
    private static void sub() {
        n++;
        sub();
    }
    public static void heapCheck() throws IOException {
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long total = rt.totalMemory();
        long free = rt.freeMemory();
        long used = total - free;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.printf("Total memory: %s%n",total);
        System.out.printf("Free memory: %s%n",free);
        System.out.printf("Used memory: %s%n",used);
        System.out.printf("Press ENTER key to continue: ");
        String str = br.readLine();
    }
}
