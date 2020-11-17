(string FundName, string SicavName) SplitFundName(string name)
{
    var idx = name.IndexOf("-");
    if (idx < 0) return (name, (string)null);
    return (name.Substring(idx + 1).Trim(), name.Substring(0, idx).Trim());
}

var rbcNavFileDefinition = FlatFileDefinition.Create(i => new
{
    // AmountRedemption = i.ToNumberColumn<double?>("AMOUNT REDEMPTION", "."),
    // AmountSubscription = i.ToNumberColumn<double?>("AMOUNT SUBSCRIPTION", "."),
    Currency = i.ToColumn<string>("CURRENCY"),
    FundCurrency = i.ToColumn<string>("FUND CURRENCY"),
    NameOfShares = i.ToColumn<string>("NAME OF SHARES"),
    FundCode = i.ToColumn<string>("FUND CODE"),
    FundName = i.ToColumn<string>("FUND NAME"),
    FundTotalNetAsset = i.ToNumberColumn<double>("FUND TOTAL NET ASSET", "."),
    IsinCode = i.ToColumn<string>("ISIN CODE"),
    NavDate = i.ToDateColumn("NAV DATE", "yyyyMMdd"),
    NavPerShare = i.ToNumberColumn<double>("NAV PER SHARE", "."),
    NbShares = i.ToNumberColumn<double?>("OUTSTANDING SHARES", "."),
    QuantityRedemption = i.ToNumberColumn<double?>("QUANTITY REDEMPTION", "."),
    QuantitySubscription = i.ToNumberColumn<double?>("QUANTITY SUBSCRIPTION", "."),
    //Tis = i.ToNumberColumn<double>("TIS", "."),
    TotalNetAssetBeforeDividend = i.ToNumberColumn<double>("TNA BEFORE DIVIDEND", "."),
    TotalNetAsset = i.ToNumberColumn<double>("TOTAL NET ASSET", "."),
    //TotalTisAmount = i.ToNumberColumn<double>("TOTAL TIS AMOUNT", "."),
}).IsColumnSeparated(',');

var navFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse nav file", rbcNavFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key")
    .Fix($"{TaskName}: Fill in missing values for old files", o => o
        .FixProperty(i => i.FundCurrency).IfNullWith(i => i.Currency)
        .FixProperty(i => i.NbShares).IfNullWith(i => i.TotalNetAsset / i.NavPerShare));

// Sicav
var sicavStream = navFileStream
    .Select($"{TaskName}: Create sicavs", ProcessContextStream, (i, ctx) => new Sicav
    {
        InternalCode = SplitFundName(i.FundName).SicavName,
        Name = SplitFundName(i.FundName).SicavName,
        IssuerId = ctx.TenantId
    })
    .Distinct($"{TaskName}: Distinct sicavs", i => i.InternalCode)
    .EfCoreSave($"{TaskName}: Insert sicav", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// Portfolio
var portfolioStream = navFileStream
    .LookupCurrency($"{TaskName}: Get related currency for portfolio", l => l.FundCurrency, (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: Get related sicav", sicavStream, (l, r) => new { l.Currency, l.FileRow, Sicav = r })
    .Select($"{TaskName}: Create portfolios", ProcessContextStream, (i, ctx) => new SubFund
    {
        InternalCode = i.FileRow.FundCode,
        Name = SplitFundName(i.FileRow.FundName).FundName,
        ShortName = SplitFundName(i.FileRow.FundName).FundName.Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        SicavId = i.Sicav?.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .Distinct($"{TaskName}: Distinct portfolios", i => i.InternalCode)
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// Shareclass
var shareClassStream = navFileStream
    .LookupCurrency($"{TaskName}: Get related currency for shareclass", l => l.Currency, (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: Get related portfolio", portfolioStream, (l, r) => new { l.Currency, l.FileRow, Portfolio = r })
    .Select($"{TaskName}: Create share classes", i => new ShareClass
    {
        InternalCode = i.FileRow.IsinCode,
        Name = $"{i.FileRow.NameOfShares} {i.FileRow.Currency}",
        ShortName = $"{i.FileRow.NameOfShares} {i.FileRow.Currency}".Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        SubFundId = i.Portfolio?.Id,
        Isin = i.FileRow.IsinCode
    })
    .Distinct($"{TaskName}: Distinct shareclass", i => i.InternalCode)
    .EfCoreSave($"{TaskName}: Insert share classes", o => o.SeekOn(i => i.Isin).AlternativelySeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// Portfolio HV
var savedPortfolioHistoricalValueStream = navFileStream
    .CorrelateToSingle($"{TaskName}: Get related portfolio for HV", portfolioStream, (l, r) => new { l.FundTotalNetAsset, l.NbShares, l.QuantitySubscription, l.QuantityRedemption, l.NavDate, PortfolioId = r.Id })
    .Pivot($"{TaskName}: Distinct portfolio and date", i => new { i.PortfolioId, i.NavDate }, i => new
    {
        TNA = AggregationOperators.First(i.FundTotalNetAsset),
        NBS = AggregationOperators.Sum(i.NbShares),
        SUB = AggregationOperators.Sum(i.QuantitySubscription),
        RED = AggregationOperators.Sum(i.QuantityRedemption),
    })
    .CrossApplyEnumerable($"{TaskName}: Unpivot portfolio historical values",
        i => new[]
            {
                new { Type = HistoricalValueType.TNA, Value = (double?)i.Aggregation.TNA },
                new { Type = HistoricalValueType.NBS, Value = (double?)i.Aggregation.NBS },
                new { Type = HistoricalValueType.RED, Value = (double?)i.Aggregation.RED },
                new { Type = HistoricalValueType.SUB, Value = (double?)i.Aggregation.SUB },
            }
            .Where(j => j.Value != null)
            .Select(j => new PortfolioHistoricalValue
            {
                PortfolioId = i.Key.PortfolioId,
                Date = i.Key.NavDate,
                Type = j.Type,
                Value = j.Value.Value,
            }))
    .EfCoreSave($"{TaskName}: Save portfolio hv", o => o.SeekOn(i => new { i.Date, i.PortfolioId, i.Type }));

// ShareClass HV
var savedShareClassHistoricalValueStream = navFileStream
    .CorrelateToSingle($"{TaskName}: Get related shareclass for HV", shareClassStream, (l, r) => new { FileRow = l, ShareClass = r })
    .Distinct($"{TaskName}: Distinct historical values", i => new { i.FileRow.NavDate, i.ShareClass.Isin })
    .CrossApplyEnumerable($"{TaskName}: Unpivot share class historical values",
        i => new[]{
            new { Type = HistoricalValueType.TNA, Value = (double?)i.FileRow.TotalNetAsset },
            new { Type = HistoricalValueType.MKT, Value = (double?)i.FileRow.NavPerShare },
            new { Type = HistoricalValueType.NBS, Value = (double?)i.FileRow.NbShares },
            new { Type = HistoricalValueType.RED, Value = (double?)i.FileRow.QuantityRedemption },
            new { Type = HistoricalValueType.SUB, Value = (double?)i.FileRow.QuantitySubscription },
	        new { Type = HistoricalValueType.DIV, Value = 
                ((double?)i.FileRow.TotalNetAssetBeforeDividend - (double?)i.FileRow.TotalNetAsset) / ((double?)i.FileRow.NbShares)},
        }
        .Where(j => j.Value != null && j.Value != 0)
        .Select(j => new SecurityHistoricalValue
        {
            SecurityId = i.ShareClass.Id,
            Date = i.FileRow.NavDate,
            Type = j.Type,
            Value = j.Value.Value
        }))
    .EfCoreSave($"{TaskName}: Save share class hv", o => o.SeekOn(i => new { i.Date, i.SecurityId, i.Type }));

return FileStream.WaitWhenDone($"{TaskName}: Wait till everything is saved", savedPortfolioHistoricalValueStream, savedShareClassHistoricalValueStream);
