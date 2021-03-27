List<BenchmarkComposition> GetBenchmarkCompositions(Portfolio portfolio, BenchmarkComposition lastComposition,
        List<PortfolioSecurityHistoricalValue> prices, List<PortfolioFxRate> fxRates)
{
    if (lastComposition==null)
        throw new Exception("Benchmark must have at least one compo");
 
    var fxRatesDic = fxRates.GroupBy(i => i.CurrencyToId).ToDictionary(i=>i.Key, i=>i);

    var compoDates = GetCompoDatesBetween(lastComposition.Date.AddWorkingDays(1), prices.Max(i => i.Date));
    //compoDates = compoDates.Take(3).OrderBy(i=>i.Date).ToList();

    var benchCompos = new List<BenchmarkComposition>();
    var currentPositions = lastComposition.Positions.Cast<BenchmarkSecurityPosition>().ToList();

    var securities = currentPositions.Select(i => i.Security).GroupBy(i => i.Id).Select(g => g.First()).ToDictionary(i => i.Id , i => i);
   
    var currentDate = lastComposition.Date;
    foreach (var newDate in compoDates)
    {
        var newPositions = new List<BenchmarkSecurityPosition>(); 
        foreach (var pos in currentPositions)
        {
            var p0 = prices.Where(i=> i.SecurityId == pos.SecurityId && i.Date <= currentDate).OrderByDescending(i=>i.Date).FirstOrDefault();
            var p1 = prices.Where(i=> i.SecurityId == pos.SecurityId && i.Date <= newDate).OrderByDescending(i=>i.Date).FirstOrDefault();

            double perf = double.NaN;
            if ((p0 != null) && (p1 != null) && (p0.Date != p1.Date) && (p0.AdjustedPrice != 0) && (p1.AdjustedPrice != 0))
            {
                if (securities[pos.SecurityId].CurrencyId == null)
                    throw new Exception($"Currency not set for {pos.Security.Name}");
                
                if (securities[pos.SecurityId].Currency.IsoCode == "GBx")
                {
                    var gbpId = fxRates.First(i => i.CurrencyToIso == "GBP").CurrencyToId;
                    var fx0 = fxRatesDic[gbpId].Where(i=> i.Date <= currentDate).OrderByDescending(i=>i.Date).FirstOrDefault();
                    var fx1 = fxRatesDic[gbpId].Where(i=> i.Date <= newDate).OrderByDescending(i=>i.Date).FirstOrDefault();
                    if (fx0 == null || fx1 == null)
                        throw new Exception($"Fx Rate Missing for {securities[pos.SecurityId].Currency.IsoCode} at Date: {currentDate}");

                    perf = ( (p1.AdjustedPrice / (fx1.Rate * 0.01) ) / (p0.AdjustedPrice / (fx0.Rate * 0.01)  )) - 1;
                }
                else if (securities[pos.SecurityId].Currency.IsoCode != "USD")
                {
                    var fx0 = fxRatesDic[securities[pos.SecurityId].CurrencyId.Value].Where(i=> i.Date <= currentDate).OrderByDescending(i=>i.Date).FirstOrDefault();
                    var fx1 = fxRatesDic[securities[pos.SecurityId].CurrencyId.Value].Where(i=> i.Date <= newDate).OrderByDescending(i=>i.Date).FirstOrDefault();
                    if (fx0 == null || fx1 == null)
                        throw new Exception($"Fx Rate Missing for {pos.Security.Currency.IsoCode} at Date: {currentDate}");
                    perf = ( (p1.AdjustedPrice / fx1.Rate) / (p0.AdjustedPrice / fx0.Rate)) - 1;
                }
                else
                    perf = (p1.AdjustedPrice / p0.AdjustedPrice) - 1;
            }
            double newWeight = (!double.IsNaN(perf) && perf < 2 && perf > -0.75 )? (pos.Weight * (1+perf)) : pos.Weight;
            newPositions.Add(new BenchmarkSecurityPosition{SecurityId = pos.SecurityId, Weight = newWeight});
        }
       
        double total = newPositions.Sum(i => i.Weight);
        foreach (var newPos in newPositions)        
            newPos.Weight = newPos.Weight / total;
        
        benchCompos.Add(new BenchmarkComposition { PortfolioId = portfolio.Id, Date = newDate, 
                        Positions = newPositions.Cast<BenchmarkPosition>().ToList()});
        currentPositions = newPositions;
        currentDate = newDate;
    }
    return benchCompos;
}

var lastBenchmarkCompositionsStream = ProcessContextStream.EfCoreSelect($"{TaskName} Get Benchmark Positions", (ctx,j) => ctx
    .Set<BenchmarkComposition>().Include(inc => inc.Positions).ThenInclude(i=>(i as BenchmarkSecurityPosition).Security).ThenInclude(i => i.Currency)
    .Where(p => p.Portfolio.InternalCode == "775400-UI"))
    .Aggregate("get Benchmark Compositions",
        i=>i.PortfolioId,
        i=>(BenchmarkComposition)null,
        (a,v)=>{
            if(a == null) return v;
            if(a.Date < v.Date) return v;
            return a;
        });

var securityAdjustedPricesStream = lastBenchmarkCompositionsStream.EfCoreSelect($"{TaskName} Get Prices", (ctx,j) => ctx
    .Set<SecurityHistoricalValue>().Where( i => i.Date >= j.Aggregation.Date && i.Type == HistoricalValueType.TRP)
    .Join(ctx.Set<BenchmarkSecurityPosition>().Include(i=>i.BenchmarkComposition).Where(i=>i.BenchmarkCompositionId == j.Aggregation.Id),
        i => i.SecurityId, i=>i.SecurityId,(l,r) => 
        new PortfolioSecurityHistoricalValue{
            PortfolioId = r.BenchmarkComposition.PortfolioId, 
            SecurityId = l.SecurityId,
            Date = l.Date,
            AdjustedPrice = l.Value,
        }
    ))
    .Aggregate("Prices List", 
        i=>i.PortfolioId, 
        i=>new List<PortfolioSecurityHistoricalValue>(),
        (a,v)=> {
            a.Add(v);
            return a;
    });

var fxRatesStream = lastBenchmarkCompositionsStream.EfCoreSelect($"{TaskName} Get FxRates", (ctx,j) => ctx
    .Set<FxRate>().Include(i=>i.CurrencyTo).Where( i => i.Date >= j.Aggregation.Date).Select(i => new PortfolioFxRate {
            PortfolioId = j.Aggregation.PortfolioId,
            Date = i.Date,
            CurrencyToId = i.CurrencyToId,
            CurrencyToIso = i.CurrencyTo.IsoCode,
            Rate = i.RateFromReferenceCurrency
        }
    ))
    .Aggregate($"{TaskName} Rates List", 
        i=>i.PortfolioId, 
        i=>new List<PortfolioFxRate>(),
        (a,v)=> {
            a.Add(v);
            return a;
    });
    
var createNewBenchmarkComposStream = ProcessContextStream.EfCoreSelect("Get Portfolios", (ctx,j) => ctx
    .Set<Portfolio>().Where(p => p.InternalCode == "775400-UI"))
    .Lookup($"{TaskName} Get related last benchmark composition", lastBenchmarkCompositionsStream, l=>l.Id, r=>r.Key, 
        (l, r) => new {PortfolioId=l.Id, Portfolio=l, BenchmarkComposition=r?.Aggregation})
    .Lookup($"{TaskName} Get related security prices", securityAdjustedPricesStream, l=>l.PortfolioId, r=>r.Key, 
        (l, r) => new {PortfolioId=l.PortfolioId, Portfolio=l.Portfolio, BenchmarkComposition = l.BenchmarkComposition,
                        Prices = r?.Aggregation})
    .Lookup($"{TaskName} Get related fx rates", fxRatesStream, l=>l.PortfolioId, r=>r.Key, 
        (l, r) => new {PortfolioId=l.PortfolioId, Portfolio=l.Portfolio, BenchmarkComposition = l.BenchmarkComposition,
                        Prices = l.Prices, FxRates = r?.Aggregation })
    .CrossApplyEnumerable($"{TaskName} Create new benchmark compositions", i=> GetBenchmarkCompositions(i.Portfolio, i.BenchmarkComposition,i.Prices,i.FxRates))
    .EfCoreDelete($"{TaskName} Delete current positions", o=>o
        .Set<BenchmarkPosition>()
        .Where((composition, position)=>position.BenchmarkComposition.Date == composition.Date 
            && position.BenchmarkComposition.PortfolioId == composition.PortfolioId ))
    .EfCoreSave($"{TaskName} Save Benchmark compos", o=>o
        .SeekOn(i=>new {i.PortfolioId, i.Date})
        .WithMode(SaveMode.EntityFrameworkCore));

//We create a "MSCI-Estimated" portfolio
// var sicavsStream = ProcessContextStream.EfCoreSelect($"{TaskName} Get sicav stream", (ctx, j) => ctx.Set<Sicav>());
// var subFundsStream = ProcessContextStream.CrossApplyEnumerable($"{TaskName}: Cross-apply Sub Fund",ctx=>
//     new [] {
//         new { InternalCode = "MSCI-Estimated", Name = "MSCI-Estimated", Currency="USD", SicavCode = "UI I"},
//     })
//     .Lookup($"{TaskName} get related sicav", sicavsStream, i => i.SicavCode, i => i.InternalCode, 
//         (l,r) => new {Row = l, Sicav = r})
//     .LookupCurrency($"{TaskName}: Create Sub Fund Ccy", i => i.Row.Currency,
//         (l,r) => new {l.Row, l.Sicav , Currency = r})
//     .Select($"{TaskName}: Create Sub Fund",i => new SubFund{
//         InternalCode = i.Row.InternalCode,
//         Name = i.Row.Name,
//         ShortName = i.Row.InternalCode,
//         CurrencyId = i.Currency.Id,
//         SicavId = i.Sicav.Id,
//         PricingFrequency = FrequencyType.Daily,
//     })
//     .EfCoreSave($"{TaskName}: Save Sub Fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists())
//     .EnsureSingle($"{TaskName}: ensures only one sub fund");

// var oldestCompoStream = createNewBenchmarkComposStream
//     .Distinct($"{TaskName} distinct min compo", i=> i.PortfolioId, o => o.ForProperty(i=> i.Date, DistinctAggregator.Min));

// var deleteNewerExistingPortfolioCompoStream = oldestCompoStream
//     .Select($"{TaskName} get target portfolio",subFundsStream, (i,j)=>
//         new {Portfolio = j, Compo = i})
//     .EfCoreDelete($"{TaskName} Existing newer compos", o => o
//         .Set<PortfolioComposition>()
//         .Where((i,j) => j.PortfolioId == i.Portfolio.Id && j.Date > i.Compo.Date));

// var portfolioCompositionStream = createNewBenchmarkComposStream
//     .WaitWhenDone($"{TaskName}: wait portfolio composition deletion", deleteNewerExistingPortfolioCompoStream)
//     .Select($"{TaskName}: create composition - msci portfolio",subFundsStream, (i,j) => new PortfolioComposition { 
//         PortfolioId = j.Id, 
//         Date = i.Date})
//     .EfCoreSave($"{TaskName}: save composition - msci portfolio", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

// var positionsStream2 = createNewBenchmarkComposStream
//     .CrossApplyEnumerable($"{TaskName}: cross apply on positions", i => i.Positions)
//     .Lookup($"{TaskName} get related port compo",portfolioCompositionStream, i=>i.BenchmarkComposition.Date, i=>i.Date, 
//         (l,r)=> new {compo = r, position = l })
//     .Select($"{TaskName}: create position", i => new Position
//         {
//             PortfolioCompositionId = i.compo.Id,
//             SecurityId = (i.position as BenchmarkSecurityPosition).SecurityId,
//             MarketValueInPortfolioCcy = i.position.Weight * 1000000,
//             Value = i.position.Weight * 1000000,
//             Weight = i.position.Weight,
//         })
//         .EfCoreSave($"{TaskName}: save positions - msci portfolio", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));

ProcessContextStream.WaitWhenDone("wait till everything is done", createNewBenchmarkComposStream);


List<DateTime> GetCompoDatesBetween(DateTime minDate, DateTime maxDate)
{
    if (maxDate == DateTime.Today)
        maxDate = maxDate.AddWorkingDays(-1);

    var holidayDates = new List<DateTime>(){}; //...TBC
    
    var calculationDates = new List<DateTime>();
    var currentDate = maxDate;
    while (currentDate >= minDate)
    {
        calculationDates.Add(currentDate);
        currentDate = currentDate.AddWorkingDays(-1);
    }
    return calculationDates.Except(holidayDates).Where(i => i >= minDate).OrderBy(i => i).ToList();
}

class PortfolioSecurityHistoricalValue
{
    public int PortfolioId {get;set;}
    public int SecurityId {get; set;}
    public DateTime Date {get;set;}
    public double AdjustedPrice {get;set;}
}

class PortfolioFxRate
{
    public int PortfolioId {get;set;}
    public DateTime Date {get;set;}
    public int CurrencyToId {get; set;}
    public string CurrencyToIso {get; set;}
    public double Rate {get;set;}
}