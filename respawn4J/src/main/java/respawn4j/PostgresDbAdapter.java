package respawn4j;

public class PostgresDbAdapter {

    private final char QUOTE_Character = '"';
    private final String InformationSchema = "information_schema";
    private final String PostgresSchemaPrefix = "pg_";

    public String buildTableCommandText(RespawnerOptions options)
    {
        String commandText = """
                select TABLE_SCHEMA, TABLE_NAME
                        from INFORMATION_SCHEMA.TABLES
                        where TABLE_TYPE = 'BASE TABLE'
                """;


        commandText += String.format(" AND TABLE_SCHEMA != '{%s}'", InformationSchema);
        commandText += String.format(" AND TABLE_SCHEMA NOT LIKE '%s%%'", PostgresSchemaPrefix);

        return commandText;
    }

    public String buildRelationshipCommandText(RespawnerOptions options) {
        var commandText = """
                select tc.table_schema, tc.table_name, ctu.table_schema, ctu.table_name, rc.constraint_name
                 from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                 inner join INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu ON rc.constraint_name = ctu.constraint_name
                 inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON rc.constraint_name = tc.constraint_name
                 where 1=1
                """;

        return commandText;
    }

    public String buildDeleteCommandText(GraphBuilder graph) {
        var builder = new StringBuilder();

        var parentTables = graph.cyclicRelationships.stream().map(Relationship::parentTable).toList();
        for (var table : parentTables) {
            var alterCommand = String.format("ALTER TABLE %s DISABLE TRIGGER ALL;", table.getFullName(QUOTE_Character));
            builder.append(alterCommand);
        }
        if(!graph.toDelete.isEmpty()) {
            var tablesToDeleteNames = graph.toDelete.stream().map(x -> x.getFullName(QUOTE_Character)).toList();
            var joinedTablesToDeleteNames = String.join(", ", tablesToDeleteNames);
            var deleteCommand = String.format("truncate table %s cascade;", joinedTablesToDeleteNames);
            builder.append(deleteCommand);
        }
        for (var table : parentTables) {
            var alterCommand = String.format("ALTER TABLE %s ENABLE TRIGGER ALL;", table.getFullName(QUOTE_Character));
            builder.append(alterCommand);
        }
        return builder.toString();
    }
}
