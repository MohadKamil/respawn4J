package respawn4j.dbTests;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import respawn4j.PostgresDbAdapter;
import respawn4j.Respawner;
import respawn4j.RespawnerOptions;
import respawn4j.Table;
import respawn4j.dbTests.connectionProviders.PostgresJDBConnectionProvider;

import java.sql.SQLException;
import java.util.UUID;


public class PostgresTests {

    static final String dbName = UUID.randomUUID().toString();
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:15-alpine"
    ).withDatabaseName(dbName);

    private PostgresJDBConnectionProvider connectionProvider;

    @BeforeAll
    static void beforeAll() {
        postgres.start();
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }

    @BeforeEach
    void setUp() {
        this.connectionProvider = new PostgresJDBConnectionProvider(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword()
        );
    }

    @Test
    void whenResettingDb_thenAllTablesAreEmpty() throws SQLException {
        var connection = connectionProvider.getConnection();

        var createTestTableStatement = connection.prepareStatement(
                "create table foo (value integer)"
        );

        createTestTableStatement.execute();

        var insertTestValueStatement = connection.prepareStatement(
                """
                    insert into foo (value) values (1), (2), (3), (4), (5)
                """
        );
        insertTestValueStatement.execute();

        var currentCountStatement = connection.prepareStatement(
                "select count(*) from foo"
        );
        var currentCountResultSet = currentCountStatement.executeQuery();
        currentCountResultSet.next();
        var currentCount = currentCountResultSet.getInt(1);
        Assertions.assertEquals(5, currentCount);
        var respawner = Respawner.create(connection, null);
        respawner.resetDb(connection);

        var newCountStatement = connection.prepareStatement(
                "select count(*) from foo"
        );
        var newCountResultSet = newCountStatement.executeQuery();
        newCountResultSet.next();
        var newCount = newCountResultSet.getInt(1);
        Assertions.assertEquals(0, newCount);
    }

    @Test
    void whenConfiguringTableToIgnore_ResetShouldNotDeleteIgnoredTable() throws SQLException {
        var connection = connectionProvider.getConnection();

        var createTestTableStatement = connection.prepareStatement(
                "create table foo (value integer)"
        );

        createTestTableStatement.execute();

        var insertTestValueStatement = connection.prepareStatement(
                """
                    insert into foo (value) values (1), (2), (3), (4), (5)
                """
        );
        insertTestValueStatement.execute();

        var currentCountStatement = connection.prepareStatement(
                "select count(*) from foo"
        );
        var currentCountResultSet = currentCountStatement.executeQuery();
        currentCountResultSet.next();
        var currentCount = currentCountResultSet.getInt(1);
        Assertions.assertEquals(5, currentCount);
        var options = new RespawnerOptions(new Table[]{
                new Table("foo")
        });
        var respawner = Respawner.create(connection, options);
        respawner.resetDb(connection);

        var newCountStatement = connection.prepareStatement(
                "select count(*) from foo"
        );
        var newCountResultSet = newCountStatement.executeQuery();
        newCountResultSet.next();
        var newCount = newCountResultSet.getInt(1);
        Assertions.assertEquals(5, newCount);
    }

}
