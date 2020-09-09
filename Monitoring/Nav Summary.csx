NewList("NAV Summary").AddRows(
Referential.Portfolios.Where(i => i.SicavEntityId == Parameters.SicavId).Select(i=>
    NewListRow($"{i.Name}", new []
	{ NewValue("Ccy").SetText(i.CcyIso),
	 NewValue("Nav Date").SetText(i.NavDate.Value.ToString("dd/MM/yyyy")),
	 NewValue("Nav").SetNumber(i.Nav),
	 NewValue("Nav T-1").SetNumber(i.PreviousNav),
	 NewValue("% Change").SetPercentage( (i.Nav/i.PreviousNav)-1 )}
	))
    .ToList()
)