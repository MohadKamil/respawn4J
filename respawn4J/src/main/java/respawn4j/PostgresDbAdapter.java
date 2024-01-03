package respawn4j;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PostgresDbAdapter {

    private final char QUOTE_Character = '"';
    private final String InformationSchema = "information_schema";
    private final String PostgresSchemaPrefix = "pg_";

    public String buildTableCommandText(RespawnerOptions options)
    {
        StringBuilder commandText = new StringBuilder("""
                select TABLE_SCHEMA, TABLE_NAME
                        from INFORMATION_SCHEMA.TABLES
                        where TABLE_TYPE = 'BASE TABLE'
                """);

        if (options.tablesToIgnore().length > 0) {
            var tablesToIgnoreGroups = Arrays.stream(options.tablesToIgnore())
                    .collect(Collectors.groupingBy(t -> t.schema() != null));

            for (var tableGroup : tablesToIgnoreGroups.entrySet()) {
                if (tableGroup.getKey()) {
                    String args = tableGroup.getValue().stream()
                            .map(table -> "'" + table.schema() + "." + table.name() + "'")
                            .collect(Collectors.joining(","));

                    commandText.append(" AND TABLE_SCHEMA || '.' || TABLE_NAME NOT IN (").append(args).append(")");
                } else {
                    String args = tableGroup.getValue().stream()
                            .map(table -> "'" + table.name() + "'")
                            .collect(Collectors.joining(","));

                    commandText.append(" AND TABLE_NAME NOT IN (").append(args).append(")");
                }
            }
        }


        commandText.append(String.format(" AND TABLE_SCHEMA != '{%s}'", InformationSchema));
        commandText.append(String.format(" AND TABLE_SCHEMA NOT LIKE '%s%%'", PostgresSchemaPrefix));

        return commandText.toString();
    }

    public String buildRelationshipCommandText(RespawnerOptions options) {
        StringBuilder commandText = new StringBuilder("""
                select tc.table_schema, tc.table_name, ctu.table_schema, ctu.table_name, rc.constraint_name
                 from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                 inner join INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu ON rc.constraint_name = ctu.constraint_name
                 inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON rc.constraint_name = tc.constraint_name
                 where 1=1
                """);

        if (options.tablesToIgnore().length > 0) {
            var tablesToIgnoreGroups = Arrays.stream(options.tablesToInclude())
                    .collect(Collectors.groupingBy(t -> t.schema() != null));

            for (var tableGroup : tablesToIgnoreGroups.entrySet()) {
                if (tableGroup.getKey()) {
                    String args = tableGroup.getValue().stream()
                            .map(table -> "'" + table.schema() + "." + table.name() + "'")
                            .collect(Collectors.joining(","));

                    commandText.append(" AND tc.TABLE_SCHEMA || '.' || tc.TABLE_NAME NOT IN (").append(args).append(")");
                } else {
                    String args = tableGroup.getValue().stream()
                            .map(table -> "'" + table.name() + "'")
                            .collect(Collectors.joining(","));

                    commandText.append(" AND tc.TABLE_NAME NOT IN (").append(args).append(")");
                }
            }
        }

        return commandText.toString();
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
