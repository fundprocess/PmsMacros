var fileDefinition = FlatFileDefinition.Create(i => new
{
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    InstrumentCode = i.ToColumn("InstrumentCode"),
    HistoricalPrice = i.ToNumberColumn<double>("HistoricalPrice", "."),
    UpdateBehavior = i.ToColumn("UpdateBehavior"),
}).IsColumnSeparated(',');


var histValFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse historical values file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var indexPricesFileStream = histValFileStream.Where($"{TaskName} where isIndexPrices",i => !i.InstrumentCode.Contains(" Curncy-XR"));
// Create Indices
var indexStream = indexPricesFileStream
    .Distinct($"{TaskName}: Distinct index", i=>i.InstrumentCode)
    //.LookupCurrency($"{TaskName}: Lookup Currency index",i=>i.IndexCcy, (l,r) => new {FileRow = l, Currency = r})
    .Select($"{TaskName}: Create indices", i=> new FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index{
            InternalCode = i.InstrumentCode,
            Name = i.InstrumentCode,
            ShortName = i.InstrumentCode,
            PricingFrequency = FrequencyType.Daily,
            //IsCurrencyRiskFree = i.FileRow.IsRiskFreeIndex,
            //ReferenceCurrencyId = i.Currency != null? i.Currency.Id : (int?) null,
            //ReferenceCountryId
        })
    .EfCoreSave($"{TaskName}: Save indices", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());
 
var saveBbgCode = indexStream
    .Select($"{TaskName}: save bbg code", i => new IndexDataProviderCode
    {
        IndexId = i.Id,
        Code = i.InternalCode,
        DataProvider = "Bloomberg",
    })
    .EfCoreSave($"{TaskName}: saveBbgCode", o => o.SeekOn(i => new { i.Code, i.IndexId, i.DataProvider }).DoNotUpdateIfExists());

var indexPricesStream = indexPricesFileStream
    .CorrelateToSingle($"{TaskName}: Get related Index",indexStream, (l,r) => new {FileRow = l, Index = r} )
    .Select($"{TaskName}: Create Index Prices", i => new IndexHistoricalValue
    {
        IndexId = i.Index.Id,
        Type = HistoricalValueType.MKT,
        Date = i.FileRow.Date,
        Value = i.FileRow.HistoricalPrice,
    })
    .EfCoreSave($"{TaskName}: Save index price ", o => o.SeekOn(i => new {i.IndexId,i.Type,i.Date}).DoNotUpdateIfExists());

//FX RATE
var fxRatesFileStream = histValFileStream.Where($"{TaskName} where isFxRates",i => i.InstrumentCode.Contains(" Curncy-XR"));
var fxRatesStream = fxRatesFileStream
	.LookupCurrency($"{TaskName}: get Fx Rate currency to", i=> i.InstrumentCode.Substring(3,3) , (l,r) => new {FileRow = l, CurrencyTo=r})
    //.Where("${TaskName} filter unexisting currency", i=>i.FileRow.XrateDate!=null && i.CurrencyTo!=null)
	.Select($"{TaskName}: Create Fx Rate", i => new FxRate{
		CurrencyToId = (i.CurrencyTo != null)? i.CurrencyTo.Id : throw new Exception($"Currency not found ({i.FileRow.InstrumentCode.Substring(3,3)})"),
		Date = i.FileRow.Date,
		RateFromReferenceCurrency = i.FileRow.HistoricalPrice, 
	})
	.EfCoreSave($"{TaskName}: Save fx rate", o => o.SeekOn(i => new {i.CurrencyToId, i.Date}).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is processed", indexPricesStream, fxRatesStream);
