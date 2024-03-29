package respawn4j;

public record RespawnerOptions(Table[] tablesToIgnore, Table[] tablesToInclude, PostgresDbAdapter dbAdapter) {

    public RespawnerOptions(Table[] tablesToIgnore) {
        this(tablesToIgnore, new Table[0], new PostgresDbAdapter());
    }

    public static RespawnerOptions empty() {
        return new RespawnerOptions(new Table[]{}, new Table[]{}, new PostgresDbAdapter());
    }
}
