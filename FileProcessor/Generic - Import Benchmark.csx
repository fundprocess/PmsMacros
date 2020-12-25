//Important notice: Overwrite mode
var fileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn("PortfolioCode"),
    PortfolioName = i.ToColumn("PortfolioName"),
    BenchmarkName = i.ToColumn("BenchmarkName"),
    ReferenceCcyIso = i.ToColumn("ReferenceCcyIso"),
    RestrikingFreq = i.ToColumn("RestrikingFreq"),
    FromDate = i.ToDateColumn("FromDate","yyyy-MM-dd"),
    IndexCode = i.ToColumn("IndexCode"),
    IndexBloombergCode = i.ToColumn("IndexBloombergCode"),
    IndexEodCode = i.ToColumn("IndexEodCode"),
    IndexName = i.ToColumn("IndexName"),
    IndexCcy = i.ToColumn("IndexCcy"),
    IsRiskFreeIndex = i.ToBooleanColumn("IsRiskFreeIndex","TRUE","FALSE"),
    Weight = i.ToNumberColumn<double>("Weight","."),
    ApplyFxOnIndex = i.ToBooleanColumn("ApplyFxOnIndex","TRUE","FALSE"),
}).IsColumnSeparated(',');

var benchmarkPositionsFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

// Create Indices
var indexStream = benchmarkPositionsFileStream
    .Distinct($"{TaskName}: Distinct index", i=>i.IndexCode)
    .LookupCurrency($"{TaskName}: Lookup Currency index",i=>i.IndexCcy, (l,r) => new {FileRow = l, Currency = r})
    .Select($"{TaskName}: Create indices", i=> new {
        BloombergCode = i.FileRow.IndexBloombergCode,
        EodCode = i.FileRow.IndexEodCode,
        Index = new FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index{
            InternalCode = i.FileRow.IndexCode,
            Name = i.FileRow.IndexName,
            ShortName = i.FileRow.IndexCode,
            PricingFrequency = FrequencyType.Daily,
            IsCurrencyRiskFree = i.FileRow.IsRiskFreeIndex,
            ReferenceCurrencyId = i.Currency != null? i.Currency.Id : (int?) null,
            //ReferenceCountryId
        } 
    })
    .EfCoreSave($"{TaskName}: Save indices", o => o
        .Entity(i=>i.Index)
        .SeekOn(i => i.InternalCode)//.DoNotUpdateIfExists()
        .Output((i,e)=> i));
 
var saveBbgCode = indexStream
    .Where($"{TaskName}: where bloomberg code", i => !string.IsNullOrEmpty(i.BloombergCode))
    .Distinct($"{TaskName}: Distinct BloombergCode", i => new {i.Index.InternalCode, i.BloombergCode} )
    .Select($"{TaskName}: save bbg code", i => new IndexDataProviderCode
    {
        IndexId = i.Index.Id,
        Code = i.BloombergCode,
        DataProvider = "Bloomberg",
    })
    .EfCoreSave($"{TaskName}: saveBbgCode", o => o.SeekOn(i => new { i.IndexId, i.DataProvider }));

var saveEodCode = indexStream
    .Where($"{TaskName}: where EOD code", i => !string.IsNullOrEmpty(i.EodCode))
    .Distinct($"{TaskName}: Distinct EOD Code", i => new {i.Index.InternalCode, i.EodCode} )
    .Select($"{TaskName}: save eod code", i => new IndexDataProviderCode
    {
        IndexId = i.Index.Id,
        Code = i.BloombergCode,
        DataProvider = "EOD",
    })
    .EfCoreSave($"{TaskName}: save EOD Code", o => o.SeekOn(i => new { i.IndexId, i.DataProvider }));

// Create Target Exposure
var targetExposuresStream = benchmarkPositionsFileStream
        .Distinct($"{TaskName}: distinct target exposures",i => i.PortfolioCode)
        .LookupPortfolio($"{TaskName}: Lookup portfolio", i => i.PortfolioCode, (l,r) => new {FileRow = l, Portfolio = r} )
        .Where($"{TaskName}: where portfolio not null", i=>i.Portfolio != null)
        .Select($"{TaskName}: Create target exposure", i => new BenchmarkTargetExposure{
            PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found (" + i.FileRow.PortfolioCode+")"),
            Name = i.FileRow.BenchmarkName,
            FromDate = i.FileRow.FromDate,
            RestrikingFrequency = (FrequencyType)Enum.Parse(typeof(FrequencyType), i.FileRow.RestrikingFreq, true),
        })
        .EfCoreSave($"{TaskName}: save target exposures", o => o.SeekOn(i => new { i.PortfolioId, i.FromDate }));

// Create Target ExposurePositions
var targetExposurePositionsStream = benchmarkPositionsFileStream
        .CorrelateToSingle($"{TaskName}: get related target exposure",targetExposuresStream,(l,r) => new {FileRow = l, TargetExposure = r})
        .CorrelateToSingle($"{TaskName}: get related index",indexStream,
            (l,r) => new {FileRow = l.FileRow, TargetExposure = l.TargetExposure, Index = r.Index})
        .Where($"{TaskName}: where target exposure not null", i => i.TargetExposure != null)
        .Select($"{TaskName}: Create target exposure position", i => new BenchmarkTargetExposureIndexPosition{
            IndexId = i.Index != null? i.Index.Id : throw new Exception("Index not found : " + i.FileRow.IndexCode),
            BenchmarkTargetExposureId = i.TargetExposure.Id,
            Weight = i.FileRow.Weight,
            ApplyFxRate = i.FileRow.ApplyFxOnIndex,
        })
        .EfCoreSave($"{TaskName}: save target exposure positions", o => o
            .SeekOn(i => new { i.BenchmarkTargetExposureId, i.IndexId }));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved",
    saveEodCode,saveBbgCode,
    targetExposurePositionsStream
);