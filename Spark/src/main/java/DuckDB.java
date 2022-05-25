import java.sql.*;

public class DuckDB {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs=stmt.executeQuery("SELECT serviceName, AVG(CPU),AVG(RAM),AVG(Disk),SUM(CPU) FROM '/home/mohamed-yasser/Projects/Spark/Parquet/*.parquet' GROUP BY serviceName;");
        while (rs.next()){
            System.out.println(rs.getString("serviceName"));
            System.out.println("CPU: "+rs.getString("avg(\"CPU\")"));
            System.out.println("RAM: "+rs.getString("avg(\"RAM\")"));
            System.out.println("Disk: "+rs.getString("avg(\"Disk\")"));
            System.out.println("sum: "+rs.getString("sum(\"CPU\")"));
        }
//        // insert two items into the table
//        stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");


    }
}
