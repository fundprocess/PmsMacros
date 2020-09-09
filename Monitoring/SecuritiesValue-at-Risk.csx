var portfolios = Referential.PortfolioSummaries.ToDictionary(i=>i.Id,i=>i);
var securities = Referential.SecuritySummaries.ToDictionary(i=>i.Id,i=>i);

NewList("Security").AddRows(
Referential.LastStatistics.Where(i => i.SecurityId.HasValue 
				&& portfolios[i.PortfolioId].SicavEntityId == Parameters.SicavId)
    .OrderByDescending(stat=>stat.MarginalGaussianVaR1M1YE.Value)
	.Select(stat=>
    NewListRow($"{securities[stat.SecurityId.Value].Name}", new []
	{ NewValue("Ccy").SetText(securities[stat.SecurityId.Value].CcyIso),
	 NewValue("Portfolio").SetText(portfolios[stat.PortfolioId].Name),
	 NewValue("Date").SetDate(stat.Date),
	 NewValue("Gauss VaR 1M1YE").SetPercentage(stat.GaussianVaR1M1YE).SetSentiment(stat.GaussianVaR1M1YE<-0.1?MacroResultSentiment.Negative:MacroResultSentiment.Positive),
	 NewValue("Gauss CVaR 1M1YE").SetPercentage(stat.GaussianExpectedShortfall1M1YE).SetSentiment(stat.GaussianExpectedShortfall1M1YE<-0.1?MacroResultSentiment.Negative:MacroResultSentiment.Positive),
	 NewValue("Gauss Marg. VaR 1M1YE").SetPercentage(stat.MarginalGaussianVaR1M1YE),
	}
	))
    .ToList()
)