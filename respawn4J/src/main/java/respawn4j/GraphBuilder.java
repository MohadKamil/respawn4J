package respawn4j;

import java.util.*;

public class GraphBuilder {
    public final Set<Relationship> cyclicRelationships;
    public final Collection<Table> toDelete;

    public GraphBuilder(HashSet<Table> tables, HashSet<Relationship> relationships) {
        fillRelationships(tables, relationships);
        var cyclesResult = findAndRemoveCycles(tables);

        toDelete = cyclesResult.toDelete().stream().toList();
        cyclicRelationships = Collections.unmodifiableSet(cyclesResult.cyclicRelationships());
    }

    private CycleResult findAndRemoveCycles(HashSet<Table> tables) {
        var notVisited = new HashSet<Table>(tables);
        var visiting = new HashSet<Table>();
        var visited = new HashSet<Table>();
        var cyclicRelationships = new HashSet<Relationship>();
        var toDelete = new Stack<Table>();

        for (var table : tables) {
            hasCycles(table, notVisited, visiting, visited, toDelete, cyclicRelationships);
        }
        return new CycleResult(cyclicRelationships, toDelete);
    }

    private record CycleResult(HashSet<Relationship> cyclicRelationships, Stack<Table> toDelete) {
    }

    private static Boolean hasCycles(Table table,
                                     HashSet<Table> notVisited,
                                     HashSet<Table> visiting,
                                     HashSet<Table> visited,
                                     Stack<Table> toDelete,
                                     HashSet<Relationship> cyclicalRelationships) {
        if (visited.contains(table))
            return false;

        if (visiting.contains(table))
            return true;

        notVisited.remove(table);
        visiting.add(table);

        var filteredRelationships = table.relationships().stream().filter(x -> hasCycles(x.referencedTable(), notVisited, visiting, visited, toDelete, cyclicalRelationships)).toList();
        cyclicalRelationships.addAll(filteredRelationships);

        visiting.remove(table);
        visited.add(table);
        toDelete.push(table);

        return false;
    }

    private void fillRelationships(HashSet<Table> tables, HashSet<Relationship> relationships) {
        for (var relationship : relationships) {
            var parentTable = tables.stream().filter(x -> x.equals(relationship.parentTable())).findFirst().orElse(null);
            var referencedTable = tables.stream().filter(x -> x.equals(relationship.referencedTable())).findFirst().orElse(null);

            if (parentTable != null && referencedTable != null && !parentTable.equals(referencedTable)) {
                var relation = new Relationship(parentTable, referencedTable, relationship.name());
                parentTable.relationships().add(relation);
            }

        }
    }
}
