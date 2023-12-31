package respawn4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;

public class Respawner {

    private final RespawnerOptions options;
    private String deleteSql;

    private Respawner(RespawnerOptions options) {

        this.options = options;
    }

    public static Respawner create(Connection connection, RespawnerOptions options) throws SQLException {

        if(options == null) {
            options = RespawnerOptions.empty();
        }
        var respawner = new Respawner(options);

        respawner.buildDeleteQuery(connection);

        return respawner;
    }

    private void buildDeleteQuery(Connection connection) throws SQLException {
        var allTables = getAllTables(connection);

        if(allTables.isEmpty()) {
            throw new IllegalArgumentException("No tables found in database");
        }

        var allRelationships = getRelationships(connection);

        var graphBuilder = new GraphBuilder(allTables, allRelationships);

        deleteSql = options.dbAdapter().buildDeleteCommandText(graphBuilder);
    }

    private HashSet<Relationship> getRelationships(Connection connection) throws SQLException {
        var relationships = new HashSet<Relationship>();
        var commandText = options.dbAdapter().buildRelationshipCommandText(options);
        var statement = connection.createStatement();
        var resultSet = statement.executeQuery(commandText);
        while (resultSet.next()) {
            var parentTableSchema = resultSet.getString(1);
            var parentTableName = resultSet.getString(2);
            var referencedTableSchema = resultSet.getString(3);
            var referencedTableName = resultSet.getString(4);
            var name = resultSet.getString(5);

            var parentTable = new Table(parentTableSchema, parentTableName);
            var referencedTable = new Table(referencedTableSchema, referencedTableName);

            relationships.add(new Relationship(parentTable, referencedTable, name));
        }

        return relationships;

    }

    private HashSet<Table> getAllTables(Connection connection) throws SQLException {
        var tables = new HashSet<Table>();

        var statement = connection.createStatement();
        var getTablesQueryTest = options.dbAdapter().buildTableCommandText(options);
        var resultSet = statement.executeQuery(getTablesQueryTest);

        while (resultSet.next()) {
            var schema = resultSet.getString("TABLE_SCHEMA");
            var name = resultSet.getString("TABLE_NAME");

            tables.add(new Table(schema, name));
        }

        return tables;
    }

    public String getDeleteSql() {
        return deleteSql;
    }

    public void resetDb(Connection connection) {
        try {
            var statement = connection.createStatement();
            statement.execute(deleteSql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
