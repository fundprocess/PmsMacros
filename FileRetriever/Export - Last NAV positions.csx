var dbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get dbStream", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.PortfolioComposition>().GroupBy(i=> i.PortfolioId)
        .Select(i=> new {PortfolioId = i.Key, Date = i.Max(d=>d.Date) })
        .Join(
            ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.PortfolioComposition>()
                .Include(i => i.Portfolio)
                .Include(i => i.Positions).ThenInclude(i => i.Security),
            i => i,
            i => new {i.PortfolioId, i.Date},
            (l,r) => r)
        );

var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("PortfolioCode"),
    PortfolioName = i.ToColumn<string>("PortfolioName"),
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    SecurityCode = i.ToColumn<string>("SecurityCode"),
    SecurityName = i.ToColumn<string>("SecurityName"),
    SecurityType = i.ToColumn<string>("SecurityType"),
    FxRate = i.ToNumberColumn<double?>("FxRate","."),
    MarketValueInSecCcy = i.ToNumberColumn<double?>("MarketValueInSecCcy","."),
    MarketValueInPortCcy = i.ToNumberColumn<double>("MarketValueInPortCcy","."),
    Weight = i.ToNumberColumn<double>("Weight", "."),
}).IsColumnSeparated(',');

var gicsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get GICS", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Classifications.Classification>());

var export = dbStream.CrossApplyEnumerable($"{TaskName}: Cross-apply Positions", i=> i.Positions)
    .Select("create extract items", i => new {
        PortfolioCode = i.PortfolioComposition.Portfolio.InternalCode,
        PortfolioName = i.PortfolioComposition.Portfolio.Name,
        Date = i.PortfolioComposition.Date,
        SecurityCode = i.Security.InternalCode,
        SecurityName = i.Security.Name,
        SecurityType = i.Security.GetType().ToString().Split(".").Last(),
        FxRate = i.MarketValueInSecurityCcy/i.MarketValueInPortfolioCcy,
        MarketValueInSecCcy = i.MarketValueInSecurityCcy,
        MarketValueInPortCcy = i.MarketValueInPortfolioCcy,
        Weight = i.Weight,
    })
    .ToTextFileValue("Export to csv", $"PortfolioNav - {DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss")}.csv", csvFileDefinition);

return export;