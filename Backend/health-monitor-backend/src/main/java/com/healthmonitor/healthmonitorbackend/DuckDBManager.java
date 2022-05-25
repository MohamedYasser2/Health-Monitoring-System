package com.healthmonitor.healthmonitorbackend;

import java.sql.*;
import java.util.ArrayList;

public class DuckDBManager {

    public ArrayList<String> queryBatchView(Long startDate, Long endDate) throws ClassNotFoundException, SQLException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:/home/hadoopuser/DuckDB/dummy");
        PreparedStatement prep = conn.prepareStatement("SELECT AVG(maxCPU), AVG(maxRAM), AVG(maxDisk), SUM(count)  FROM '/home/hadoopuser/DuckDB/*.parquet' WHERE stamp BETWEEN ? AND ? GROUP BY serviceName;");
        prep.setDouble(1, startDate);
        prep.setDouble(2, endDate);
        Statement stmt = conn.createStatement();
        ResultSet rs = prep.executeQuery();
        System.out.println("here");
        System.out.println(rs);
        ArrayList<String> strings = new ArrayList<>();
        while (rs.next()) {
            strings.add("CPU: " + rs.getString(1) + "\tRAM: " + rs.getString(2) + "\tDisk: " + rs.getString(3) + "\tServices Count: " + rs.getString(4));
        }
        rs.close();
        return strings;
    }

}
