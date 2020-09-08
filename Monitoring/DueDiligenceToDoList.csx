NewList("Due diligences - To Do List")
    .AddRows(
        Referential.DueDiligences
            .Where(i=>i.RelationshipType == "InvestorRelationship" && i.EndValidity>=DateTime.Today)
            .Select(i=>NewListRow($"{i.EntityId}", new []
            { 
                NewValue("Nb Remaining Mandatory Tasks").SetInteger(i.NbRemainingMandatoryTasks),
            })).ToList())
