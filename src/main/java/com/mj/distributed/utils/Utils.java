package com.mj.distributed.utils;

public class Utils {

    public static int majority(int num) {
        return (num/2)+1 ;
    }

    public static int getRandomDelay() {

        /* int random = (int)Math.random()*7 ;
        return 913 + 417*random ; */

        return 913 + (int)(Math.random()*1000) ;

    }

}
