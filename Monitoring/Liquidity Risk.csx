var portfolios=Referential.Portfolios.Where(i => i.SicavEntityId == Parameters.SicavId && i.NavDate.HasValue).ToList();
var shareClasses=Referential.InternalShareClasses.Where(i=>i.PortfolioId.HasValue).ToList();
//var securities=Referential.SecuritySummaries.ToDictionary(i=>i.Id,i=>i);

var liabilitySideRows = new List<MacroResultRow>();
foreach (var port in portfolios)
{
	var dateFrom = port.NavDate.Value.AddMonths(-1);
	var shareClassesIds = shareClasses.Where(i=>i.PortfolioId == port.Id).Select(i=>i.Id);
	var totalSubscriptions = Referential.ShareClassHistValues.Where(i=> shareClassesIds.Contains(i.ShareClassId) && i.Date>dateFrom).Select(i=>i.Sub).Sum();
	var totalRedemptions = Referential.ShareClassHistValues.Where(i=> shareClassesIds.Contains(i.ShareClassId) && i.Date>dateFrom).Select(i=>i.Red).Sum();
	
	var row = NewListRow(port.Name, new []
					{
					NewValue("NAV Date").SetDate(port.NavDate), 
					NewValue("Total subscription").SetNumber(totalSubscriptions),
					NewValue("Total redemption").SetNumber(totalRedemptions),
                    NewValue("Net cash In/Out").SetNumber(totalSubscriptions-totalRedemptions),
					});
	liabilitySideRows.Add(row);
}


var assetSideRows = new List<MacroResultRow>();
foreach (var port in portfolios)
{
	var row = NewListRow(port.Name, new []
					{ NewValue("1 Day").SetPercentage(0.5),
					NewValue("2 Days").SetPercentage(0.2),
					NewValue("3 Days").SetPercentage(0.1),
					NewValue("5 Days").SetPercentage(0.1),
					NewValue("10 Days").SetPercentage(0.05),
					NewValue(">=10 Days").SetPercentage(0.05),
					});
	assetSideRows.Add(row);
}

var section = NewSection("Liquidity Risk");
section.AddList("Liability side",liabilitySideRows);
section.AddList("Asset side - Liquidation times",assetSideRows);
section