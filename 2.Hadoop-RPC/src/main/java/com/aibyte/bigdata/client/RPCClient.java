package com.aibyte.bigdata.client;

import com.aibyte.bigdata.Interface.IQueryStudent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RPCClient {
    public static void main(String[] args) {
        try {
            IQueryStudent proxy = RPC.getProxy(IQueryStudent.class, 1L, new InetSocketAddress("127.0.0.1", 3000), new Configuration());
            String name = proxy.findName("G20210735010084");
            System.out.println("student G20210735010084 name :-> " + name);
            String notExisted = proxy.findName("20210000000000");
            System.out.println("student 20210000000000 name :-> " + notExisted);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
