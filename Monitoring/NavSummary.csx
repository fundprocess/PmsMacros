NewList("NAV Summary").AddRows(
    Referential.Portfolios
        .Where(i => i.SicavEntityId == Parameters.SicavId)
        .Select(i =>
            NewListRow($"{i.Name}", new[] {
                NewValue("Ccy").SetText(i.CcyIso),
                NewValue("Nav Date").SetDate(i.NavDate.Value),
                NewValue("Nav").SetNumber(i.Nav),
                NewValue("Nav T-1").SetNumber(i.PreviousNav),
                NewValue("% Change").SetPercentage( (i.Nav/i.PreviousNav)-1 )}))
        .ToList()
)
//.SetSentiment(i.Position.Weight.Choose(MacroResultSentiment.Positive, 0.1, MacroResultSentiment.Negative))