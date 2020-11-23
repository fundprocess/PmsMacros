//1. FILE MAPPING
var kbcPositionFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioInternalCode = i.ToColumn<string>("CLIENT NBR"),
    Isin = i.ToColumn<string>("ISIN"),
    SecInternalCode = i.ToColumn<string>("SEC NBR"),
    SecName = i.ToColumn<string>("SEC NAME"),
    SecCur = i.ToColumn<string>("SEC CUR"),
    SecType = i.ToColumn<string>("SEC TYPE"),
    Quantity = i.ToNumberColumn<double>("POSITION", "."),
    MarketValueInSecurityCcy = i.ToNumberColumn<double?>("VALUE SEC INT IN", "."),
    MarketValueInPortfolioCcy = i.ToNumberColumn<double?>("VAL PORT INT INC", "."),
    //InputPositionDate = i.ToColumn("PRICE DATE"),
    //EntryDate = i.ToDateColumn("POS_DATE", "yyyyMMdd"),
    PositionDate = i.ToDateColumn("DATE", "yyyyMMdd"),
    FxRate = i.ToNumberColumn<double?>("EXCH RATE", "."),
    Price = i.ToNumberColumn<double?>("PRICE", "."),
    FileName = i.ToSourceName()
}).IsColumnSeparated(',');

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse position file", kbcPositionFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//.FixProperty(p => p.PosDate).IfNullWith(i => (!DateTime.TryParseExact(i.FileName.Substring(0, 10), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date) ? (DateTime?)null : date)))

//2. CREATE PORTFOLIOS
// Portfolio
var euroCurrencyStream = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var portfolioStream = posFileStream
    .Select($"{TaskName}: Create portfolios", euroCurrencyStream, (l, r) => new DiscretionaryPortfolio
    {
        InternalCode = l.PortfolioInternalCode,
        Name = l.PortfolioInternalCode,
        ShortName = "KBC",
        CurrencyId = r.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .Distinct($"{TaskName}: Distinct portfolios", i => i.InternalCode)
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//2. CREATE TARGET SECURITIES
var targetSecurityStream = posFileStream
        .Distinct($"{TaskName}: distinct position securities", i => i.SecInternalCode)
        .LookupCurrency($"{TaskName}: get related currency", l => l.SecCur, (l, r) => new { FileRow = l, Currency = r })
        //CreateSecurity(string internalCode, string secType, string secName, int? currencyId, string isin)
        .Select($"{TaskName}: create target security", i => CreateSecurity(i.FileRow.SecInternalCode, i.FileRow.SecType,
                        i.FileRow.SecName, i.Currency?.Id, i.FileRow.Isin))
        .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());


var portfolioCompositionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) =>
            new PortfolioComposition { Date = l.PositionDate, PortfolioId = r.Id })
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioId, i.Date }, true)
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r)
                        => new { fileRow = l.FileRow, sec = l.Security, compo = r })
    .Select($"{TaskName}: create position", i => new Position
    {
        SecurityId = i.sec.Id,
        PortfolioCompositionId = i.compo.Id,
        MarketValueInPortfolioCcy = i.fileRow.MarketValueInPortfolioCcy.Value,
        MarketValueInSecurityCcy = i.fileRow.MarketValueInSecurityCcy,
        Value = i.fileRow.Quantity,
        ValuationPrice = i.fileRow.Price,
        //CostPrice
        //BookCostInSecurityCcy
        //BookCostInPortfolioCcy
        //ProfitLossOnMarketPortfolioCcy
        //ProfitLossOnFxPortfolioCcy
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));
return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream);


Security CreateSecurity(string internalCode, string secType, string secName, int? currencyId, string isin)
{
    if (string.IsNullOrWhiteSpace(secType) && !string.IsNullOrWhiteSpace(isin)) secType = "FUND";
    Security security = null;
    switch (secType)
    {
        case "FUND":
        case "TRACKER":
            security = new ShareClass();
            break;
        case "BOND":
            security = new Bond();
            break;
        case "SHARE":
        case "RIGHT":
            security = new Equity();
            break;
        case "COUPON":
            security = new Cash();
            break;
    }

    if (security != null)
    {
        security.InternalCode = (!string.IsNullOrEmpty(isin)) ? isin : internalCode;
        security.CurrencyId = currencyId;
        if (security is OptionFuture der)
            der.UnderlyingIsin = isin;
        else if (security is SecurityInstrument securityInstrument)
            securityInstrument.Isin = isin;
        security.Name = secName;
        security.ShortName = secName.Truncate(MaxLengths.ShortName);
    }

    return security;
}
