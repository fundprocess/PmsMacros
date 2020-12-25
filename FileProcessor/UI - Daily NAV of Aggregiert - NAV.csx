//File Definition
var navFileDefinition = FlatFileDefinition.Create(i => new
{
    FundNumber = i.ToColumn<string>("Fund number"),
    Isin = i.ToColumn<string>("ISIN"),
    Name = i.ToColumn<string>("Name"),
    ValuationDate = i.ToDateColumn("Valuation date", "dd.MM.yyyy"),
    NavPerShare = i.ToNumberColumn<double?>("NAV per share", ","),
    OutstandingShares = i.ToNumberColumn<double?>("Outstanding Shares", ","),
    SubscriptionPrice = i.ToNumberColumn<double?>("Subscription Price", ","),
    FundDailyPerformance = i.ToNumberColumn<double?>("Fund daily performance", ","),
    BenchmarkDailyPerformance = i.ToNumberColumn<double?>("Benchmark daily performance", ","),
    Withdrawal = i.ToNumberColumn<double?>("Withdrawal", ","),
    Beta = i.ToNumberColumn<double?>("Beta", ","),
    //PortfolioTurnoverRate = i.ToColumn<string>("Portfolio Turnover Rate", ","),
    TotalNetAsset = i.ToNumberColumn<double?>("NAV", ","),
    Currency = i.ToColumn<string>("Currency"),
}).IsColumnSeparated(';');

#region FileStream
var navFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse nav file", navFileDefinition)
    .SetForCorrelation($"{TaskName}: prepare correlation");

var subFundTnaFileStream = navFileStream
    .Where($"{TaskName}: where subFundTnaStream", i => string.IsNullOrEmpty(i.Isin));

var shareClassNavFileStream = navFileStream
    .Where($"{TaskName}: where shareClassNavStream", i => !string.IsNullOrEmpty(i.Isin));
#endregion FileStream

var sicavsStream = subFundTnaFileStream
    .Distinct($"{TaskName} Distinct", i => i.Name.Split(" - ").First())
    .LookupCurrency($"{TaskName}: get related SICAV currency",i => i.Currency, 
        (l,r) => new {FileRow = l,  Currency = r })
    .Select($"{TaskName}: Create sicav ",ProcessContextStream, (i,ctx) => new Sicav{
        InternalCode = i.FileRow.Name.Split(" - ").First(),
        Name = i.FileRow.Name.Split(" - ").First(),
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,        
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
        IssuerId = ctx.TenantId
    })
    .EfCoreSave($"{TaskName}: save sicav", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

string getPortfolioInternalCode(string fundNumber)
    => fundNumber + "-UI";

var subfundsStream = subFundTnaFileStream
    .Distinct($"{TaskName} distinct sub fund", i => i.FundNumber)
    .LookupCurrency($"{TaskName}: get related Sub fund currency", i => i.Currency , 
        (l,r) => new {FileRow = l,  Currency = r })
    .CorrelateToSingle($"{TaskName}: get related SICAV",sicavsStream,
        (l,r) => new {FileRow = l.FileRow,  Currency = l.Currency, Sicav = r })
    .Select($"{TaskName}: Create target subFund ", i => new SubFund{
        InternalCode = getPortfolioInternalCode(i.FileRow.FundNumber),
        Name =  i.FileRow.Name,
        ShortName = getPortfolioInternalCode(i.FileRow.FundNumber),
        PricingFrequency = FrequencyType.Daily,
        SicavId = (i.Sicav != null)? i.Sicav.Id : throw new Exception("Sicav not found for: " + i.FileRow.Name),
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,
    })
    .EfCoreSave($"{TaskName}: save target sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var shareClassStream = shareClassNavFileStream
    .Distinct($"{TaskName}: distinct share classes", i => i.Isin)
    .LookupCurrency($"{TaskName}: get related currency for share class", l => l.Currency,
            (l, r) => new { FileRow = l, Currency = r })
    .Lookup($"{TaskName}: lookup related sub fund", subfundsStream, 
            i => getPortfolioInternalCode(i.FileRow.FundNumber.Substring(0, 4)+"00"), i => i.InternalCode,
            (l, r) => new { FileRow = l.FileRow,Currency = l.Currency, SubFund = r })
    .Select($"{TaskName}: create share class", i => new {
        TotalNetAsset = i.FileRow.OutstandingShares * i.FileRow.NavPerShare, //TODO: replace by i.FileRow.TotalNetAsset when available
        ShareClass= new ShareClass
        {
            InternalCode = i.FileRow.Isin,
            Name = i.FileRow.Name,
            ShortName = i.FileRow.Name.Split(" - ").Last(),
            CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,
            Isin = i.FileRow.Isin,
            IsOpenForInvestment = true,
            SubFundId = (i.SubFund != null)? i.SubFund.Id : throw new Exception("Sub fund not found for: "+i.FileRow.Name),
        }})
    .EfCoreSave($"{TaskName}: Save share classes", o => o
            .Entity(i=>i.ShareClass)
            .SeekOn(i => i.Isin).AlternativelySeekOn(i => i.InternalCode)
            .Output((i,j)=> i)
            .DoNotUpdateIfExists());

var savedShareClassHvStream = shareClassNavFileStream
    .Distinct($"{TaskName}: distinct nav per shareclass", i => new { i.ValuationDate, i.Isin })
    .CorrelateToSingle($"{TaskName}: get related share class", shareClassStream, 
        (l, r) => new { FileRow = l, ShareClass = r.ShareClass })
    .CrossApplyEnumerable($"{TaskName}: unpivot share class historical values", i => new[]{
        new { Shareclass = i.ShareClass, Date = i.FileRow.ValuationDate, Isin = i.FileRow.Isin, 
                Type = HistoricalValueType.TNA, Value = (i.FileRow.OutstandingShares * i.FileRow.NavPerShare) },//i.FileRow.TotalNetAsset
        new { Shareclass = i.ShareClass, Date = i.FileRow.ValuationDate, Isin = i.FileRow.Isin, 
                Type = HistoricalValueType.MKT, Value = i.FileRow.NavPerShare },
        new { Shareclass = i.ShareClass, Date = i.FileRow.ValuationDate, Isin = i.FileRow.Isin, 
                Type = HistoricalValueType.NBS, Value = i.FileRow.OutstandingShares }
    })
    .Where($"{TaskName}: Exclude empty value", i => i.Value.HasValue)
    .Select($"{TaskName}: Create share class hv", i => new { 
        SubFundId = i.Shareclass.SubFundId.Value,
        hv = new SecurityHistoricalValue
        {
            SecurityId = i.Shareclass.Id,
            Date = i.Date,
            Type = i.Type,
            Value = i.Value.Value
        }})
    .EfCoreSave($"{TaskName}: save share class hv", o => o
        .Entity(i=>i.hv).SeekOn(i => new { i.Date, i.SecurityId, i.Type }).DoNotUpdateIfExists()
        .Output((i,j)=> i));

var fundTnaStream = savedShareClassHvStream
    .Where($"{TaskName}: keep tna value", i => i.hv.Type == HistoricalValueType.TNA)
    .Aggregate($"{TaskName}: Sum share class tna",
        i => new { SubFundId = i.SubFundId, Date = i.hv.Date},
        i => new { Tna = (double ) 0},
        (cur, i) => new {Tna = cur.Tna + i.hv.Value})
    .Select($"{TaskName}: Create portfolio hv", i => new PortfolioHistoricalValue
    {
        PortfolioId = i.Key.SubFundId,
        Date = i.Key.Date,
        Type = HistoricalValueType.TNA,
        Value = i.Aggregation.Tna,
    })
    .EfCoreSave($"{TaskName}: save portfolio tna", o => o.SeekOn(i => new { i.PortfolioId, i.Date, i.Type }));


return FileStream.WaitWhenDone($"{TaskName}: wait till every hv is saved", fundTnaStream
,savedShareClassHvStream,subfundsStream,shareClassStream,sicavsStream);