package com.healthmonitor.healthmonitorbackend;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class DuckDBManager {

    public ArrayList<String> queryBatchView(Long startDate, Long endDate) throws ClassNotFoundException, SQLException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:/home/hadoopuser/DuckDB/dummy");
        PreparedStatement prep = conn.prepareStatement("SELECT serviceName, AVG(maxCPU), AVG(maxRAM), AVG(maxDisk), SUM(count)  FROM '/home/hadoopuser/DuckDB/*.parquet' WHERE stamp BETWEEN ? AND ? GROUP BY serviceName;");
        prep.setDouble(1, startDate);
        prep.setDouble(2, endDate);
        ResultSet rs = prep.executeQuery();
        String[] services = new String[]{"service-1", "service-2", "service-3", "service-4"};
        ArrayList<String> strings = new ArrayList<>();
        Set<String> set = new HashSet<>();
        while (rs.next()) {
            strings.add(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4)+ "," + rs.getString(5));
            set.add(rs.getString(1));
            System.out.println("Count CPU " + rs.getString(5));
        }
        rs.close();
        for (String s:
                services) {
            if (!set.contains(s)) {
                strings.add(s + "," + "0" + "," + "0" + "," + "0" + "," + "0");
            }

        }
        return strings;
    }


    public ArrayList<String> queryRealtimeView(Long startDate, Long endDate) throws ClassNotFoundException, SQLException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        PreparedStatement prep = conn.prepareStatement("SELECT serviceName, AVG(CPU),AVG(RAM),AVG(Disk),SUM(CPU), COUNT(CPU) FROM '/media/hadoopuser/College/College Labs/Big Data Systems/Health-Monitoring-System/Spark/Parquet/*.parquet' WHERE stamp BETWEEN ? AND ? GROUP BY serviceName;");
        prep.setDouble(1, startDate);
        prep.setDouble(2, endDate);
        ResultSet rs = prep.executeQuery();

//        ResultSet rs=stmt.executeQuery("SELECT serviceName, AVG(CPU),AVG(RAM),AVG(Disk),SUM(CPU) FROM '/home/mohamed-yasser/Projects/Spark/Parquet/*.parquet' GROUP BY serviceName;");
        ArrayList<String> strings = new ArrayList<>();
        String[] services = new String[]{"service-1", "service-2", "service-3", "service-4"};
        Set<String> set = new HashSet<>();
        while (rs.next()) {
            strings.add(rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5));
            set.add(rs.getString(1));
            System.out.println("Count CPU " + rs.getString("count(\"CPU\")"
            ));
        }
        rs.close();
        for (String s:
             services) {
            if (!set.contains(s)) {
                strings.add(s + "," + "0" + "," + "0" + "," + "0" + "," + "0");
            }

        }
        return strings;
    }

    public ArrayList<String> combineTwoQueries(ArrayList<String> batchResult, ArrayList<String> realtimeResult) throws ClassNotFoundException, SQLException {
        ArrayList<String> result = new ArrayList<>();
        for (int i = 0; i < batchResult.size(); i++) {
            String[] batch = batchResult.get(i).split(",");
            String[] realtime = realtimeResult.get(i).split(",");
            result.add("Service Name: " + batch[0] + "CPU: " + ((Double.parseDouble(batch[1]) + Double.parseDouble(realtime[1])) / 2 ) + "\tRAM: " +((Double.parseDouble(batch[2]) + Double.parseDouble(realtime[2])) / 2 ) + "\tDisk: " + ((Double.parseDouble(batch[3]) + Double.parseDouble(realtime[3])) / 2 ) + "\tServices Count: " + Math.round(Double.parseDouble(realtime[4])));
        }
        return result;
    }
}
