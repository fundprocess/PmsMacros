var fileDefinition = FlatFileDefinition.Create(i => new
{
    FundCode = i.ToColumn("FundCode"),
    Isin = i.ToColumn("ISIN"),
    BloombergName = i.ToColumn("Bloomberg Name"),
    BBGID = i.ToColumn("BBG ID"),
    Weight = i.ToNumberColumn<double?>("Weight", "."),
    ESG = i.ToNumberColumn<double?>("ESG", "."),
    Currency = i.ToColumn("Currency"),
    Date = i.ToDateColumn("Date", "dd/MM/yyyy"),
    CountryName = i.ToColumn("CountryName"),
    CountryIso2 = i.ToColumn("CountryIso2"),
    GicsName = i.ToColumn("GicsName"),
    GicsCode = i.ToColumn("GicsCode"),
}).IsColumnSeparated(',');//.WithEncoding(System.Text.Encoding.GetEncoding(1252));

var msciFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse persons file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//Create securities
var securitiesStream = msciFileStream
    .Distinct($"{TaskName}: distinct positions security", i => i.Isin)
    .LookupCurrency($"{TaskName}: get related currency", l => l.Currency,
        (l, r) => new { FileRow = l, Currency = r })
    .LookupCountry($"{TaskName}: get related country", l => l.FileRow.CountryIso2,
        (l, r) => new { FileRow = l.FileRow , Currency = l.Currency, Country = r })
    .Select($"{TaskName}: create instrument", i => new Equity{
        InternalCode = i.FileRow.Isin,
        Name = i.FileRow.BloombergName,
        ShortName = i.FileRow.BBGID,
        Isin = i.FileRow.Isin,
        CurrencyId = (i.Currency != null)? i.Currency.Id : throw new Exception("Currency not found: " + i.FileRow.Currency),
        CountryId = (i.Country != null)? i.Country.Id : throw new Exception("Country not found: " + i.FileRow.CountryIso2),
        PricingFrequency = FrequencyType.Daily,
    })
    .EfCoreSave($"{TaskName}: save target instrument", o => o.SeekOn(i => i.Isin).DoNotUpdateIfExists());
    
//Bloomberg Code
var saveBbgCode = msciFileStream
    .Where($"{TaskName} Filter empty bbg code", i=> !string.IsNullOrEmpty(i.BBGID))
    .Distinct($"{TaskName}: distinct positions security BBG Code", i => i.BBGID)
    .CorrelateToSingle($"{TaskName}: get related security bbg code", securitiesStream, 
        (l, r) => new { FileRow = l, Security = r })
    .Select($"{TaskName}: create Bloomberg code", i => new SecurityDataProviderCode
    {
        SecurityId = (i.Security != null)? i.Security.Id : throw new Exception("ShareClass not found: "+ i.FileRow.Isin),
        Code = i.FileRow.BBGID,
        DataProvider = "Bloomberg",
    })
    .EfCoreSave($"{TaskName}: save Bbg Code", o => o.SeekOn(i => new { i.Code, i.SecurityId, i.DataProvider }));

//ESG
var esgStream = msciFileStream
    .Distinct($"{TaskName}: distinct positions security ESG", i => i.Isin)
    .CorrelateToSingle($"{TaskName}: get related security ESG", securitiesStream, 
        (l, r) => new { FileRow = l, Security = r })
    .Where($"{TaskName}: where ESG has value", i => i.FileRow.ESG.HasValue)
    .Select($"{TaskName}: create ESG historical value", i=> new SecurityHistoricalValue
    {
        SecurityId = i.Security != null? i.Security.Id : throw new Exception("ESG related security not found: "+ i.FileRow.Isin),
        Date = i.FileRow.Date,
        Type = HistoricalValueType.ESG,
        Value = i.FileRow.ESG.Value,
    })
    .Distinct($"{TaskName}: Distinct security prices", i => new {i.SecurityId,i.Type,i.Date})
    .EfCoreSave($"{TaskName}: Save security prices", o => o.SeekOn(i => new {i.SecurityId,i.Type,i.Date}).DoNotUpdateIfExists());

//Import Benchmark Compos
var distinctMinBenchmarkComposStream = msciFileStream
    .Distinct($"{TaskName} distinct FundCode-Date", i=> i.FundCode, o => o.ForProperty(i=> i.Date, DistinctAggregator.Min));

var deleteNewerExistingBenchmarkCompoStream = distinctMinBenchmarkComposStream
    .EfCoreDelete($"{TaskName} Existing newer compos", o => o
        .Set<BenchmarkComposition>()
        .Where((i,j) => j.Portfolio.InternalCode == i.FundCode && j.Date >= i.Date));

var benchCompositionStream = msciFileStream
    .WaitWhenDone($"{TaskName}: wait composition deletion", deleteNewerExistingBenchmarkCompoStream)
    .Distinct($"{TaskName}: distinct composition", i => new { i.FundCode, i.Date }, true)
    .EfCoreLookup($"{TaskName}: get related subfund", o => o
        .Set<SubFund>().On(i => i.FundCode, i => i.InternalCode)
        .Select((l, r) => new { FileRow = l, SubFund = r }).CacheFullDataset())
    .Select($"{TaskName}: create bench compo", i=> new BenchmarkComposition{
        PortfolioId = i.SubFund.Id,
        Date = i.FileRow.Date
    })
    .EfCoreSave($"{TaskName}: save bench compositions", o => o.SeekOn(i => new { i.PortfolioId, i.Date }).DoNotUpdateIfExists());

//Benchmark Positions
var benchPositionsStream = msciFileStream
    .CorrelateToSingle($"{TaskName}: get related security 2", securitiesStream, 
        (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related bench composition", benchCompositionStream, 
        (l, r) => new { l.FileRow, Security = l.Security, Composition = r })
    .Select($"{TaskName}: create positions", i => new BenchmarkSecurityPosition
    {
        BenchmarkCompositionId = i.Composition.Id,
        SecurityId = i.Security.Id,
        Weight = i.FileRow.Weight.HasValue? i.FileRow.Weight.Value : throw new Exception("Weight not provided for: " + i.FileRow.Isin),
    })
    .EfCoreSave($"{TaskName}: save bench positions", o => o.SeekOn(i => new { i.SecurityId, i.BenchmarkCompositionId }));

//Assign GICS sector
string classificationTypeCode = "GICS";
var gicsType = ProcessContextStream.EfCoreSelect("Get gics classification type", (ctx,j) => ctx
    .Set<ClassificationType>().Where(ct => ct.Code == classificationTypeCode))
    .EnsureSingle($"Ensure only one gics type exists");

var gicsClassificationsStream = ProcessContextStream.EfCoreSelect("Get gics classifications", (ctx,j) => ctx
    .Set<Classification>().Include(i=>i.ClassificationType)
    .Where(c => c.ClassificationType.Code == classificationTypeCode));

var assignGics = msciFileStream
    .CorrelateToSingle($"{TaskName} get related security", securitiesStream, 
        (l,r) => new {FileRow = l, Security = r} )
    .Lookup($"{TaskName} get related classification",gicsClassificationsStream, i => i.FileRow.GicsCode, i => i.Code,
        (l,r) => new {FileRow = l.FileRow, Security = l.Security, Classification = r})
    .Where($"{TaskName} Gics not empty", i => i.Classification != null)
    .Select($"{TaskName} create classification",gicsType, (i,j) => new ClassificationOfSecurity
        {
            SecurityId = i.Security.Id,
            ClassificationId = i.Classification.Id,
            ClassificationTypeId = j.Id,
        })
    .EfCoreSave("Save gics security classifications");

// //We create an MSCI-Estimated portfolio
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

// //Import Compos
// var deleteNewerExistingPortfolioCompoStream = distinctMinBenchmarkComposStream
//     .EfCoreDelete($"{TaskName} Existing newer compos 2", o => o
//         .Set<PortfolioComposition>()
//         .Where((i,j) => j.Portfolio.InternalCode == i.FundCode && j.Date >= i.Date));

// var portfolioCompositionStream = msciFileStream
//     .WaitWhenDone($"{TaskName}: wait newer portoflio compositions deletion", deleteNewerExistingPortfolioCompoStream)
//     .Distinct($"{TaskName}: distinct composition - msci portfolio", i => new { i.FundCode, i.Date })
//     .Select($"{TaskName}: create composition - msci portfolio",subFundsStream, (i,j) => new PortfolioComposition { 
//         PortfolioId = j.Id, 
//         Date = i.Date})
//     .EfCoreSave($"{TaskName}: save composition - msci portfolio", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

// var positionsStream2 = msciFileStream
//     .CorrelateToSingle($"{TaskName}: get related security for position - msci portfolio", securitiesStream, 
//         (l, r) => new { FileRow = l, Security = r })
//     .CorrelateToSingle($"{TaskName}: get related composition for position - msci portfolio", portfolioCompositionStream, 
//         (l, r) => new { FileRow = l.FileRow, Security = l.Security, Composition = r })
//     .Select($"{TaskName}: create position", i => new Position
//         {
//             PortfolioCompositionId = i.Composition.Id,
//             SecurityId = i.Security.Id,
//             MarketValueInPortfolioCcy = i.FileRow.Weight.Value * 10000,
//             //Value = 1,
//             Weight = i.FileRow.Weight.Value,
//         })
//         .EfCoreSave($"{TaskName}: save positions - msci portfolio", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", securitiesStream, benchPositionsStream, 
esgStream, saveBbgCode, assignGics) ;