var portfolios=Referential.Portfolios.Where(i => i.SicavEntityId == Parameters.SicavId).ToList();
var securities=Referential.SecuritySummaries.ToDictionary(i=>i.Id,i=>i);
var tab = NewList("Diversification - Transferable Securities 5/40");

foreach (var port in portfolios)
{
	double sum = Referential.LastPositions
					.Where(position=>position.PortfolioId == port.Id 
							&& securities[position.SecurityId].Type != "Cash"
							&& position.Weight>0.05)
					.Select(i=>i.Weight.Value).Sum();
	var row = NewListRow($"{port.Name}", new []
					{ NewValue("Ccy").SetText(port.CcyIso),
					NewValue("Sum 5%").SetPercentage(sum).SetSentiment(sum<0.3?MacroResultSentiment.Positive:MacroResultSentiment.Negative)
					});
	tab.AddRows( new List<MacroResultRow>(){row});
}
tab