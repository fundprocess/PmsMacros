var names = Referential.Entities.ToDictionary(i=>i.Id,i=>i.Name); 
NewList("Due diligences - TO DO LIST")
    .AddRows(
        Referential.DueDiligences
            .Where(i=>i.RelationshipType == "InvestorRelationship" && i.EndValidity>=DateTime.Today)
            .Select(i=>NewListRow($"{names[i.EntityId]}", new []
            { 
                NewValue("Nb Remaining Mandatory Tasks").SetInteger(i.NbRemainingMandatoryTasks),
            })).ToList())
