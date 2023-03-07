package com.mj.raft.client;

import java.util.List;

public class LogReader {

    public static void main(String[] args) throws Exception {

        RaftClient rc = new RaftClient("localhost",5001) ;
        rc.connect();

        int start = 0;
        int count = 10 ;

        while(true) {
            StringBuilder b = new StringBuilder() ;
            List<byte[]> entries = rc.get(start, count) ;
            if (entries.size() > 0) {
                b.append("log::") ;
                entries.forEach((e)-> b.append(new String(e)).append(','));
                System.out.println(b.toString());
                start = start + entries.size();
                entries = null ;
            } else {
                System.out.println("No entries for start =" + start + " count= " + count);
            }
            Thread.sleep(5000) ;
        }

    }


}
