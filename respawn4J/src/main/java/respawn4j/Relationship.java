package respawn4j;

public record Relationship(Table parentTable, Table referencedTable, String name) {
}
