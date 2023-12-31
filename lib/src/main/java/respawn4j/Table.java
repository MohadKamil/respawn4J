package respawn4j;


import java.util.ArrayList;

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
}
