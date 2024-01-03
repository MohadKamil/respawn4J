package respawn4j;

public record RespawnerOptions(Table[] tablesToIgnore, Table[] tablesToInclude, PostgresDbAdapter dbAdapter) {

    public static RespawnerOptions empty() {
        return new RespawnerOptions(new Table[]{}, new Table[]{}, new PostgresDbAdapter());
    }
}
