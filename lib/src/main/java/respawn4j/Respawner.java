package respawn4j;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashSet;

public class Respawner {

    private final RespawnerOptions options;
    private String deleteSql;

    private Respawner(RespawnerOptions options) {

        this.options = options;
    }

    public static Respawner create(DataSource dataSource, RespawnerOptions options) throws SQLException {

        if(options == null) {
            options = RespawnerOptions.empty();
        }
        var respawner = new Respawner(options);

        respawner.buildDeleteQuery(dataSource);

        return respawner;
    }

    private void buildDeleteQuery(DataSource dataSource) throws SQLException {
        var allTables = getAllTables(dataSource);

        if(allTables.isEmpty()) {
            throw new IllegalArgumentException("No tables found in database");
        }

        var allRelationships = getRelationships(dataSource);

        var graphBuilder = new GraphBuilder(allTables, allRelationships);

        deleteSql = options.dbAdapter().buildDeleteCommandText(graphBuilder);
    }

    private HashSet<Relationship> getRelationships(DataSource dataSource) throws SQLException {
        var relationships = new HashSet<Relationship>();
        var commandText = options.dbAdapter().buildRelationshipCommandText(options);
        var connection = dataSource.getConnection();
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

    private HashSet<Table> getAllTables(DataSource dataSource) throws SQLException {
        var tables = new HashSet<Table>();

        var connection = dataSource.getConnection();
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
}
