package com.aibyte.bigdata.Interface.impl;

import com.aibyte.bigdata.Interface.IQueryStudent;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class QueryStudentImpl implements IQueryStudent {
    @Override
    public String findName(String studentID) {
        System.out.println("server received request for invoke method findName() studentID = " + studentID);
        return "G20210735010084".equalsIgnoreCase(studentID) ? "李瑞" : null;
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return IQueryStudent.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(IQueryStudent.versionID, null);
    }
}
