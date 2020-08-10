NewList("Due diligences - To Do List")
    .AddRows(
        Referential.DueDiligences
            .Where(i => i.RelationshipType == "Investor" && i.EndValidity >= DateTime.Today)
            .Select(i => NewListRow($"{i.EntityId}", new[]
                {
                    NewValue("Nb Remaining Mandatory Tasks").SetNumber(i.NbRemainingMandatoryTasks),
                })).ToList())
