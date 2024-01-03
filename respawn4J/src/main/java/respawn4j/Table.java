package respawn4j;


import java.util.ArrayList;
import java.util.Objects;

public record Table(String schema, String name, ArrayList<Relationship> relationships) {

    public Table(String schema, String name) {
        this(schema, name, new ArrayList<>());
    }

    public String getFullName(char quoteCharacter) {
        if(schema == null || schema.isBlank()) {
            return quoteCharacter + name + quoteCharacter;
        }
        return quoteCharacter + schema + quoteCharacter + "." + quoteCharacter + name + quoteCharacter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return Objects.equals(schema, table.schema) && Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name);
    }
}
