package com.aibyte.bigdata;

import com.aibyte.bigdata.Interface.IQueryStudent;
import com.aibyte.bigdata.Interface.impl.QueryStudentImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

public class RPCServer {
    public static void main(String[] args) {
        final String ADDRESS = "127.0.0.1";
        final int PORT = 3000;
        final int NUM_HANDLERS = 5;
        try {
            Server server = new RPC.Builder(new Configuration())
                    .setProtocol(IQueryStudent.class).setInstance(new QueryStudentImpl())
                    .setBindAddress(ADDRESS).setPort(PORT).setNumHandlers(NUM_HANDLERS).build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
