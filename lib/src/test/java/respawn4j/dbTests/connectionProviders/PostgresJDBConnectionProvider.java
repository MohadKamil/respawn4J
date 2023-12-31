package respawn4j.dbTests.connectionProviders;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgresJDBConnectionProvider  {

    private final String url;
    private final String username;
    private final String password;

    public PostgresJDBConnectionProvider(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
