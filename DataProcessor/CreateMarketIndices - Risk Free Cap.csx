//var riskFreeIndexCodes = new Dictionary<string, string> { ["Euribor3M"] = "EUR", ["LiborUSD3M"] = "USD", ["LiborGBP3M"] ="GBP"};
var riskFreeIndexCodes = new Dictionary<string, string> { ["Euribor3M"] = "EUR", ["US3M.GBOND"] = "USD"};

var ratesStream = ProcessContextStream.EfCoreSelect($"{TaskName} Get Rates", (ctx,j) => ctx
    .Set<IndexHistoricalValue>().Include(i => i.Index).Where(i=> riskFreeIndexCodes.Keys.Contains(i.Index.InternalCode)))
    .Aggregate("Rates List", 
        i=>i.IndexId, 
        i=>new List<IndexHistoricalValue>(),
        (a,v)=> {
            a.Add(v);
            return a;
    });

var existingIndexStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get index stream", 
        (ctx, j) => ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index>()
        .Where(i => riskFreeIndexCodes.Keys.Contains(i.InternalCode)));

var computePricesStream = ratesStream
        .Lookup($"{TaskName}: Get related index",existingIndexStream,i => i.Key, i=> i.Id,
            (l,r) => new {Index = r, Rates = l.Aggregation} )
        .LookupCurrency($"{TaskName} Get related currency", i => riskFreeIndexCodes[i.Index.InternalCode],
            (l,r) => new {Index = l.Index, Rates = l.Rates , Currency = r} )
        .Select($"{TaskName}: Create new Index Instances", i => new {
            Prices = i.Rates.ComputePrices(i.Index.InternalCode+"-CAP"),
            Index = new FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index{
                    InternalCode = i.Index.InternalCode+"-CAP",
                    Name = i.Index.Name+"-CAP",
                    ShortName = i.Index.InternalCode+"-CAP",
                    PricingFrequency = FrequencyType.Daily,
                    IsCurrencyRiskFree = true,
                    ReferenceCurrencyId = i.Currency.Id,
        }})
        .EfCoreSave($"{TaskName}: Save new Index Instances", o=>o.Entity(i => i.Index).SeekOn(i => i.InternalCode).Output((i,e)=>i)); 

var newIndexStream = computePricesStream.Select($"{TaskName}: new index stream",i => i.Index);

var deleteExistingPrices = newIndexStream.EfCoreDelete($"{TaskName}: Delete IndexHistoricalValue", o => o
        .Set<IndexHistoricalValue>()
        .Where((i, j) => i.Id == j.IndexId));

var savePrices = computePricesStream
        .WaitWhenDone($"{TaskName}: wait for existing prices deletion", deleteExistingPrices)
        .CrossApplyEnumerable($"{TaskName}: Cross-apply prices", i=> i.Prices)
        .Lookup($"{TaskName}: get related new index",newIndexStream, i => i.IndexCode, i => i.InternalCode,
            (l,r) => new { Price = l, NewIndex = r} )
        .Select($"{TaskName}: Create Prices", i => new IndexHistoricalValue{
            IndexId = i.NewIndex.Id,
            Type = HistoricalValueType.MKT,
            Date = i.Price.Date,
            Value = i.Price.Price
        })
        .EfCoreSave($"{TaskName}: Save index cap prices ", o=>o.SeekOn(i => new {i.IndexId,i.Type,i.Date})); 

ProcessContextStream.WaitWhenDone("wait till everything is done",computePricesStream,savePrices);

class NewPrice
{
    public string IndexCode {get;set;}
    public DateTime Date {get;set;}
    public double Price {get;set;}
}

static List<NewPrice> ComputePrices(this List<IndexHistoricalValue> rates, string newIndexCode)
{
    if (!rates.Any())
        return new List<NewPrice>();

    var prices = new List<NewPrice>();

    rates = rates.OrderBy(i => i.Date).ToList();
    double currentPrice = 100.0;
    DateTime currentDate = rates.First().Date;
    
    foreach (var rate in rates.Skip(1))
    {
        prices.Add(new NewPrice{IndexCode = newIndexCode, Date = currentDate, Price = currentPrice});
        
        var nbDays = (rate.Date - currentDate.Date).TotalDays;
        currentPrice = currentPrice * (1 + Math.Pow((rate.Value / 100.0), nbDays) / 360.0); 
        currentDate = rate.Date;
    }

    //var newPrice = lastPrice.Value * (1 + Math.Pow((rate.Value / 100.0), nbDays) / 360.0);
    return prices;
}