package com.aibyte.bigdata.Interface;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IQueryStudent extends VersionedProtocol {
    long versionID = 1L;
    String findName(String studentID);
}
