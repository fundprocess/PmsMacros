NewList("Top Positions").AddRows(
Referential.LastPositions
    .Join(Referential.PortfolioSummaries, position=>position.PortfolioId, portfolio=>portfolio.Id, (l,r)=> new {Portfolio=r, Position=l})
    .Join(Referential.SecuritySummaries, i=>i.Position.SecurityId, security=>security.Id, (l,r)=> new {l.Portfolio,l.Position, Security=r})
    .Where(i => i.Portfolio.SicavEntityId == Parameters.SicavId)
    .OrderByDescending(i=>i.Position.Weight)
    .Take(10)
    .Select(i=>
    NewListRow($"{i.Portfolio.Name} : {i.Security.Name}", 
        new []{ NewValue("Weight").SetPercentage(i.Position.Weight).SetSentiment(i.Position.Weight.Choose(MacroResultSentiment.Positive, 0.1, MacroResultSentiment.Negative))}))
    .ToList()
)