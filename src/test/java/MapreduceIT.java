import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Created by const on 1/24/16.
 */
public class MapreduceIT {

    @Test
    public void testMapReduce() throws SQLException {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();
            return;
        }
        Connection c = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/mydb", "SA", "");
        ResultSet rs = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY).executeQuery("select count(*) from CASE_HISTORY");
        rs.first();
        assertEquals(5, rs.getInt("C1"));

    }

}
