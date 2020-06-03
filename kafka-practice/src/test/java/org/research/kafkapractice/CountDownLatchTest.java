package org.research.kafkapractice;

import java.util.concurrent.CountDownLatch;

/**
 * @fileName: CountDownLatchTest.java
 * @description: CountDownLatchTest.java类说明
 * @author: by echo huang
 * @date: 2020-05-10 17:55
 */
public class CountDownLatchTest {
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);


    public static void main(String[] args) throws InterruptedException {
        System.out.println(1 % 100);
        try {

            System.out.println(countDownLatch.getCount());


        } finally {
            countDownLatch.countDown();
            System.out.println(countDownLatch.getCount());
        }

    }
}
