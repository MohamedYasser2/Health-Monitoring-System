package com.healthmonitor.healthmonitorbackend;

import java.sql.*;

public class DuckDBManager {

    public void initializeConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:/home/hadoopuser/DuckDB/dummy");
//        con.createAppender("main", "people");
//        appender.beginRow();
//        appender.append("Mark");
//        appender.endRow();
//        appender.close();
//
//        String query = "EXPORT DATABASE 'jdbc:duckdb:/home/hadoopuser/DuckDB/' (FORMAT PARQUET);";

        Statement stmt = conn.createStatement();
//        stmt.executeQuery(query);

//        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10,2), count INTEGER)");
        // insert two items into the table
        stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");

        ResultSet rs = stmt.executeQuery("SELECT * FROM '/home/hadoopuser/DuckDB/Data/*.parquet'");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }


//        ResultSet rs = stmt.executeQuery("SELECT * FROM items");
//        while (rs.next()) {
//            System.out.println(rs.getString(1));
//            System.out.println(rs.getInt(3));
//        }
//        rs.close();
    }

}
