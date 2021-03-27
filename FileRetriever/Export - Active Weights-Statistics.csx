

var positionsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get last positions", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.PortfolioComposition>().GroupBy(i=> i.PortfolioId)
        .Select(i=> new {PortfolioId = i.Key, Date = i.Max(d=>d.Date) })
        .Join(ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.PortfolioComposition>()
                .Include(i => i.Portfolio)
                .Include(i => i.Positions).ThenInclude(i => i.Security).ThenInclude(i => i.Currency),
            i => i,
            i => new {i.PortfolioId, i.Date},
            (l,r) => r)
        )
        .CrossApplyEnumerable($"{TaskName}: Cross-apply Positions", i=> i.Positions)
        .Where($"{TaskName} Exclude cash positions", i => !(i.Security is Cash))
        .Select($"{TaskName} Create positionTmp 1", i => 
            new PositionTmp {Portfolio = i.PortfolioComposition.Portfolio, Security = i.Security,
                            Id = i.Id,Date = i.PortfolioComposition.Date, Weight = i.Weight }  ) ;

var benchmarkPositionsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get last Benchmark positions", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.BenchmarkComposition>().GroupBy(i=> i.PortfolioId)
        .Select(i=> new {PortfolioId = i.Key, Date = i.Max(d=>d.Date) })
        .Join(ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.BenchmarkComposition>()
                .Include(i => i.Portfolio)
                .Include(i => i.Positions).ThenInclude(i => (i as BenchmarkSecurityPosition).Security).ThenInclude(i => i.Currency),
            i => i,
            i => new {i.PortfolioId, i.Date},
            (l,r) => r)
        )
        .CrossApplyEnumerable($"{TaskName}: Cross-apply Benchmark Positions", i=> i.Positions)
        .Where($"{TaskName} Where is BenchmarkSecurityPosition", i => (i is BenchmarkSecurityPosition))
        .Select($"{TaskName} Create positionTmp 2", i => 
            new PositionTmp {Portfolio = i.BenchmarkComposition.Portfolio, Security = (i as BenchmarkSecurityPosition).Security,
                            Id = i.Id,Date = i.BenchmarkComposition.Date, Weight = i.Weight }  );

var positionsUnionDistinctStream = positionsStream
    .Union($"{TaskName} Union Benchmark Securities", benchmarkPositionsStream)
        .Distinct($"{TaskName} securityPricesStream> Distinct on Securities", i => i.Security.Id);

#region Historical Values
var securityPricesStream = positionsUnionDistinctStream
    .EfCoreSelect($"{TaskName} Get last prices", (ctx,j) => ctx
        .Set<SecurityHistoricalValue>().Where( i => i.SecurityId == j.Security.Id && i.Date <= j.Date && i.Type == HistoricalValueType.MKT))
    .Aggregate($"{TaskName} Prices List", 
        i => i.SecurityId, 
        i => (SecurityHistoricalValue) null,
        (a,v)=> {
            if(a == null) return v;
            if(v.Date > a.Date) return v;
            return a;
    });

var marketCapsStream = positionsUnionDistinctStream
    .EfCoreSelect($"{TaskName} Get last market cap", (ctx,j) => ctx
        .Set<SecurityHistoricalValue>().Where( i => i.SecurityId == j.Security.Id && i.Date <= j.Date && i.Type == HistoricalValueType.CAP))
    .Aggregate($"{TaskName} market cap list", 
        i => i.SecurityId, 
        i => (SecurityHistoricalValue) null,
        (a,v)=> {
            if(a == null) return v;
            if(v.Date > a.Date) return v;
            return a;
    });

var esgStream = positionsUnionDistinctStream
    .EfCoreSelect($"{TaskName} Get last esg", (ctx,j) => ctx
        .Set<SecurityHistoricalValue>().Where( i => i.SecurityId == j.Security.Id && i.Date <= j.Date && i.Type == HistoricalValueType.ESG))
    .Aggregate($"{TaskName} esg list", 
        i => i.SecurityId, 
        i => (SecurityHistoricalValue) null,
        (a,v)=> {
            if(a == null) return v;
            if(v.Date > a.Date) return v;
            return a;
    });
#endregion

#region Statistics
//Performance attribution
var perfAttributionStreamPortfolioPositionsStream = positionsStream
    .EfCoreSelect($"{TaskName} Get last port positions PATTs", (ctx,j) => ctx
        .Set<PositionStatisticalValue>().Where( i => i.PositionId == j.Id && i.Type == HistoricalValueType.PATT
                && i.HoldingPeriod == Period.OneDay && i.CalculationPeriod == Period.MonthToDate && !i.IsAnnualized));
var perfAttributionStreamBenchmarkPositionsStream = benchmarkPositionsStream
    .EfCoreSelect($"{TaskName} Get last bench positions PATTs", (ctx,j) => ctx
        .Set<BenchmarkPositionStatisticalValue>().Where( i => i.BenchmarkPosition.Id == j.Id && i.Type == HistoricalValueType.PATT
                && i.HoldingPeriod == Period.OneDay && i.CalculationPeriod == Period.MonthToDate && !i.IsAnnualized));

//Brinson
var brinsonMTDStreamPortfolioPositionsStream = positionsStream
    .EfCoreSelect($"{TaskName} Get last port positions Brinsons", (ctx,j) => ctx
        .Set<PositionStatisticalValue>().Where( i => i.PositionId == j.Id && i.Type == HistoricalValueType.BRIN
                && i.HoldingPeriod == Period.OneDay && i.CalculationPeriod == Period.MonthToDate && !i.IsAnnualized));
var brinsonMTDStreamBenchmarkPositionsStream = benchmarkPositionsStream
    .EfCoreSelect($"{TaskName} Get last bench positions Brinsons", (ctx,j) => ctx
        .Set<BenchmarkPositionStatisticalValue>().Where( i => i.BenchmarkPosition.Id == j.Id && i.Type == HistoricalValueType.BRIN
                && i.HoldingPeriod == Period.OneDay && i.CalculationPeriod == Period.MonthToDate && !i.IsAnnualized));

//MarginalRiskOfAdding1pInBp (Inc TERR)
var incrementalTerrsStreamPortfolioPositionsStream = positionsStream
    .EfCoreSelect($"{TaskName} Get last port positions ITEs", (ctx,j) => ctx
        .Set<PositionStatisticalValue>().Where( i => i.PositionId == j.Id && i.Type == HistoricalValueType.ITE
                && i.HoldingPeriod == Period.FiveDays && i.CalculationPeriod == Period.OneYear && !i.IsAnnualized));
var incrementalTerrsStreamBenchmarkPositions = benchmarkPositionsStream
    .EfCoreSelect($"{TaskName} Get last bench positions ITEs", (ctx,j) => ctx
        .Set<BenchmarkPositionStatisticalValue>().Where( i => i.BenchmarkPosition.Id == j.Id && i.Type == HistoricalValueType.ITE
                && i.HoldingPeriod == Period.FiveDays && i.CalculationPeriod == Period.OneYear && !i.IsAnnualized));

//Contributions TERR (Contributions TERR)
var contributionTerrsStreamPortfolioPositionsStream = positionsStream
    .EfCoreSelect($"{TaskName} Get last port positions CTEs", (ctx,j) => ctx
        .Set<PositionStatisticalValue>().Where( i => i.PositionId == j.Id && i.Type == HistoricalValueType.CTE
                && i.HoldingPeriod == Period.FiveDays && i.CalculationPeriod == Period.OneYear && !i.IsAnnualized));
var contributionTerrsStreamBenchmarkPositions = benchmarkPositionsStream
    .EfCoreSelect($"{TaskName} Get last bench positions CTEs", (ctx,j) => ctx
        .Set<BenchmarkPositionStatisticalValue>().Where( i => i.BenchmarkPosition.Id == j.Id && i.Type == HistoricalValueType.CTE
                && i.HoldingPeriod == Period.FiveDays && i.CalculationPeriod == Period.OneYear && !i.IsAnnualized));

var portfolioAnnTerrsStream = positionsStream
    .Distinct($"{TaskName} distinct on port", i => i.Portfolio.Id)
    .EfCoreSelect($"{TaskName} Get last port TERR", (ctx,j) => ctx
        .Set<PortfolioStatisticalValue>().Include(i=>i.PortfolioStatistics)
        .Where( i => i.PortfolioStatistics.PortfolioId == j.Portfolio.Id && i.PortfolioStatistics.Date == j.Date
                && i.Type == HistoricalValueType.TERR
                && i.HoldingPeriod == Period.FiveDays 
                && i.Scope == StatisticScope.Exposure
                && i.CalculationPeriod == Period.OneYear && i.IsAnnualized));

#endregion
#region FxRates
var fxRatesStream = positionsUnionDistinctStream
    .Where($"{TaskName} where currency id not null" , i => i.Security.CurrencyId != null)
    .Distinct($"{TaskName} Distinct on currency id" , i => i.Security.CurrencyId)
    .EfCoreSelect($"{TaskName} Get last fx rate", (ctx,j) => ctx
        .Set<FxRate>().Include(i=>i.CurrencyTo).Where( i => i.Date <= j.Date))
    .Aggregate($"{TaskName} fx rates list", 
        i => i.CurrencyTo.IsoCode, 
        i => (FxRate) null,
        (a,v)=> {
            if(a == null) return v;
            if(v.Date > a.Date) return v;
            return a;
    });
#endregion

var posSecuritiesStream = positionsStream
        .Select($"{TaskName} get pos securities id", 
        i=> new MergedPosition{Portfolio = i.Portfolio, Security = i.Security, PortPositionId = i.Id, BenchPositionId = null} );

var benchSecuritiesStream =  benchmarkPositionsStream
        .Select($"{TaskName} get bench pos securities id", 
        i=> new MergedPosition{Portfolio = i.Portfolio, Security = i.Security , PortPositionId = null, BenchPositionId = i.Id} );

var portfolioSecuritiesStream = posSecuritiesStream
        .Union($"{TaskName} Union Benchmark Securities 2", benchSecuritiesStream)
        .Aggregate($"{TaskName} agg pos", 
            i => new {portid = i.Portfolio.Id, secid = i.Security.Id}, 
            i => (MergedPosition) null,
            (a,v) => {
                if (a == null) return v;
                if (a != null && v != null) 
                    return new MergedPosition{Portfolio = v.Portfolio, Security = v.Security, 
                        PortPositionId = a.PortPositionId.HasValue? a.PortPositionId:v.PortPositionId, 
                        BenchPositionId = a.BenchPositionId.HasValue? a.BenchPositionId:v.BenchPositionId};
                throw new Exception("MergedPosition error");
            })
        .Select($"{TaskName} get agg", i => i.Aggregation);

var activeWeightsStream = portfolioSecuritiesStream
    .Lookup($"{TaskName} Lookup positionsStream", positionsStream, i=> i.PortPositionId, i => i.Id,
        (l,r) => new {FileRow = l, Security = l.Security, PortfolioPosition = r })
    .Lookup($"{TaskName} Lookup benchmarkpositionsStream",benchmarkPositionsStream, i=> i.FileRow.BenchPositionId, i => i.Id,
        (l,r) => new {l.FileRow, l.Security, l.PortfolioPosition, 
                Portfolio = l.PortfolioPosition != null? l.PortfolioPosition.Portfolio : (r!=null? r.Portfolio:null),     
                BenchmarkPosition = r })
                
    .Lookup($"{TaskName} Lookup Brinson port", brinsonMTDStreamPortfolioPositionsStream, 
        i => i.FileRow.PortPositionId, i => i.PositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition, BrinsonPort = r})
    .Lookup($"{TaskName} Lookup Brinson bench",brinsonMTDStreamBenchmarkPositionsStream,
        i => i.FileRow.BenchPositionId, i => i.BenchmarkPositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition, 
            Brinson = (l.BrinsonPort !=null? l.BrinsonPort.Value : (r!=null? r.Value:(double?) null))})

    .Lookup($"{TaskName} Lookup Perf attrib port", perfAttributionStreamPortfolioPositionsStream, 
        i => i.FileRow.PortPositionId, i => i.PositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson, PortPerfAttrib = r})
    .Lookup($"{TaskName} Lookup Perf attrib bench", perfAttributionStreamBenchmarkPositionsStream,
        i => i.FileRow.BenchPositionId, i => i.BenchmarkPositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson, 
            PerfAttrib = (l.PortPerfAttrib !=null? l.PortPerfAttrib.Value : (r!=null? r.Value:(double?) null))})

    .Lookup($"{TaskName} Lookup incrementalTerrsStreamPortfolioPositionsStream", incrementalTerrsStreamPortfolioPositionsStream, 
        i => i.FileRow.PortPositionId, i => i.PositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, PortIncTerr = r})
    .Lookup($"{TaskName} Lookup incrementalTerrsStreamBenchmarkPositions", incrementalTerrsStreamBenchmarkPositions,
        i => i.FileRow.BenchPositionId, i => i.BenchmarkPositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, 
            IncTerr = (l.PortIncTerr !=null? l.PortIncTerr.Value : (r!=null? r.Value:(double?) null))})
    
    .Lookup($"{TaskName} Lookup contributionTerrsStreamPortfolioPositionsStream", contributionTerrsStreamPortfolioPositionsStream, 
        i => i.FileRow.PortPositionId, i => i.PositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, l.IncTerr, PortContrTerr = r})
    .Lookup($"{TaskName} Lookup contributionTerrsStreamBenchmarkPositions", contributionTerrsStreamBenchmarkPositions,
        i => i.FileRow.BenchPositionId, i => i.BenchmarkPositionId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, l.IncTerr, 
            ContrTerr = (l.PortContrTerr !=null? l.PortContrTerr.Value : (r!=null? r.Value:(double?) null))})

    .Lookup($"{TaskName} Lookup portfolioAnnTerrsStream", portfolioAnnTerrsStream,
        i => i.FileRow.Portfolio.Id, i => i.PortfolioStatistics.PortfolioId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, l.IncTerr, l.ContrTerr, 
                    AnnPortfolioTerr = (r != null)? r: throw new Exception($"PortId {l.FileRow.Portfolio.Id}") })

    .Lookup($"{TaskName} Lookup fxRatesStream", fxRatesStream, 
        i=> (i.Security.Currency.IsoCode != "GBx")? i.Security.Currency.IsoCode : "GBP" , i => i.Key,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, 
            l.IncTerr,l.ContrTerr, l.AnnPortfolioTerr, FxRate = r?.Aggregation})

    .Lookup($"{TaskName} Lookup securityPricesStream",securityPricesStream, 
        i=> i.Security.Id, i => i.Aggregation.SecurityId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, 
            l.IncTerr,l.ContrTerr, l.AnnPortfolioTerr, l.FxRate,Price = r?.Aggregation })
    .Lookup($"{TaskName} Lookup marketCapsStream", marketCapsStream, 
        i=> i.Security.Id, i => i.Aggregation.SecurityId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, 
            l.IncTerr,l.ContrTerr, l.AnnPortfolioTerr, l.FxRate,l.Price, MarketCap = r?.Aggregation })
    .Lookup($"{TaskName} Lookup esgStream", esgStream, 
        i=> i.Security.Id, i => i.Aggregation.SecurityId,
        (l,r) => new {l.FileRow, l.Portfolio, l.Security, l.PortfolioPosition, l.BenchmarkPosition,l.Brinson,l.PerfAttrib, 
            l.IncTerr,l.ContrTerr, l.AnnPortfolioTerr, l.FxRate,l.Price, l.MarketCap, Esg = r?.Aggregation });
    
var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("PortfolioCode"), 
    SecurityCode = i.ToColumn<string>("SecurityCode"),
    Name = i.ToColumn<string>("Name"),
    
    PortfolioWeight = i.ToNumberColumn<double>("PortfolioWeight","."),
    BenchmarkWeight = i.ToNumberColumn<double>("BenchmarkWeight","."),
    ActiveWeight = i.ToNumberColumn<double>("ActiveWeight","."),
    ContributionToTrackingErrorInBp = i.ToNumberColumn<double?>("ContributionToTrackingErrorInBp","."), 
    MarginalRiskOfAdding1pInBp = i.ToNumberColumn<double?>("MarginalRiskOfAdding1pInBp","."),
    ESG = i.ToNumberColumn<double>("ESG","."),
    MktCapInPortfolioCurrency = i.ToNumberColumn<double>("MktCapInPortfolioCurrency","."),
    Ccy = i.ToColumn<string>("Ccy"),
    PriceInSecurityCurrency = i.ToNumberColumn<double>("PriceInSecurityCurrency","."),
    PriceDate = i.ToOptionalDateColumn("PriceDate", "yyyy-MM-dd"),
    PriceInPortfolioCurrency = i.ToNumberColumn<double>("PriceInPortfolioCurrency","."),
    
    FxRateDate = i.ToOptionalDateColumn("FxRateDate", "yyyy-MM-dd"),
    FxRate = i.ToNumberColumn<double>("FxRate","."),

    AnnualizedPortfolioTERRInBp = i.ToNumberColumn<double?>("AnnualizedPortfolioTERRInBp","."),

    PerformanceAttributionMtdInBp = i.ToNumberColumn<double?>("PerformanceAttributionMtdInBp","."),
    BrinsonMtdInBp = i.ToNumberColumn<double?>("BrinsonMtdInBp","."),    
}).IsColumnSeparated(',');

var export = activeWeightsStream
    .Select($"{TaskName} create extract items", i => new {

        PortfolioCode = i.Portfolio.InternalCode,
        SecurityCode = i.Security.InternalCode, 
        Name = i.Security.Name,

        PortfolioWeight = i.PortfolioPosition != null? i.PortfolioPosition.Weight : 0.0, 
        BenchmarkWeight = i.BenchmarkPosition != null? i.BenchmarkPosition.Weight : 0.0,
        ActiveWeight = (i.PortfolioPosition != null? i.PortfolioPosition.Weight : 0.0)
                    - (i.BenchmarkPosition != null? i.BenchmarkPosition.Weight : 0.0),


        ContributionToTrackingErrorInBp =  10000 * i.ContrTerr, //(incTerr * (weightInPortfolio - weightInBenchmark)) / portfolioTERR;
        MarginalRiskOfAdding1pInBp = 10000 * i.IncTerr,
        ESG = i.Esg != null? i.Esg.Value : double.NaN,
        MktCapInPortfolioCurrency = i.MarketCap != null? i.MarketCap.Value : double.NaN,
        Ccy = i.Security.Currency.IsoCode,
        PriceInSecurityCurrency = i.Price != null? i.Price.Value : double.NaN,
        PriceDate = i.Price != null? i.Price.Date : (DateTime?) null,
        PriceInPortfolioCurrency = ( i.Price != null? i.Price.Value : double.NaN) /
                                (i.Security.CurrencyId == i.Portfolio.CurrencyId ? 1.0
                : (i.FxRate != null?
                    (i.Security.Currency.IsoCode == "GBx"? i.FxRate.RateFromReferenceCurrency * 100: i.FxRate.RateFromReferenceCurrency)
                    : double.NaN)),        
        
        FxRateDate = i.FxRate != null? i.FxRate.Date : (DateTime?) null,
        FxRate = i.Security.CurrencyId == i.Portfolio.CurrencyId ? 1.0
                : (i.FxRate != null?
                    (i.Security.Currency.IsoCode == "GBx"? i.FxRate.RateFromReferenceCurrency * 100: i.FxRate.RateFromReferenceCurrency)
                    : double.NaN),
            
        AnnualizedPortfolioTERRInBp = 10000 * i.AnnPortfolioTerr?.Value,
        
        PerformanceAttributionMtdInBp = 10000* i.PerfAttrib,
        BrinsonMtdInBp = 10000 * i.Brinson,
        
    })
    .ToTextFileValue("Export to csv", $"ActiveWeights - {DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss")}.csv", csvFileDefinition);

return export;

#region Helpers
class PositionTmp
{
    public Portfolio Portfolio {get;set;} 
    public Security Security {get;set;} 
    public int Id {get;set;} 
    public DateTime Date {get;set;} 
    public double Weight {get;set;} 
}
class MergedPosition
{
    public Portfolio Portfolio {get; set;}
    public Security Security {get; set;}
    public int? PortPositionId {get; set;}
    public int? BenchPositionId {get; set;}
}
#endregion