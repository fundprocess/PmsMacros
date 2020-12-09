#region FILE DEFINITION
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
#endregion

#region STREAMS
var navFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse nav file", rbcNavFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key")
    .Fix($"{TaskName}: Fill in missing values for old files", o => o
        .FixProperty(i => i.FundCurrency).IfNullWith(i => i.Currency)
        .FixProperty(i => i.NbShares).IfNullWith(i => i.TotalNetAsset / i.NavPerShare));

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var luxembourgCountry = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get Luxembourg Country", (ctx, j) => ctx.Set<Country>().Where(c => c.IsoCode2 == "LU"))
    .EnsureSingle($"{TaskName}: ensures only one LU Country");

#endregion

#region SICAV
var sicavStream = navFileStream
    .Distinct($"{TaskName}: Distinct sicavs", i => SplitFundName(i.FundName).SicavName)
    .Select($"{TaskName}: Get SICAV Euro Ccy ", euroCurrency, (i, j) => new {FileRow = i, EuroCcy = j, FundCode = i.FundCode})
    .Select($"{TaskName}: Get SICAV Luxembourg Country",luxembourgCountry, (l,r) => new {l.FileRow, l.EuroCcy , Country = r })
    .Select($"{TaskName}: Create sicavs", ProcessContextStream, (i, ctx) => new Sicav
    {
        InternalCode = SplitFundName(i.FileRow.FundName).SicavName,
        Name = SplitFundName(i.FileRow.FundName).SicavName,
        IssuerId = ctx.TenantId,
        Culture = new CultureInfo("en"),
        CountryId = i.Country.Id,
        CurrencyId = i.EuroCcy.Id,
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: Insert sicav", o => o.SeekOn(i => i.InternalCode).AlternativelySeekOn(i => i.Name).DoNotUpdateIfExists());
#endregion

#region SUBFUNDS
var subfundsStream = navFileStream
    .Distinct($"{TaskName}: Distinct portfolios", i => i.FundCode)
    .LookupCurrency($"{TaskName}: Get related currency for portfolio", l => l.Currency, (l, r) => new { FileRow = l, Currency = r })
    .Select($"{TaskName}: Get Sub-Fund Luxembourg Country",luxembourgCountry, (l,r) => new {FileRow = l.FileRow, Currency = l.Currency, Country = r })
    .CorrelateToSingle($"{TaskName}: Get related sicav", sicavStream, (l, r) => new { l.FileRow, l.Currency, l.Country, Sicav = r })
    .Select($"{TaskName}: Create portfolios", ProcessContextStream, (i, ctx) => new SubFund
    {
        InternalCode = i.FileRow.FundCode,
        Name = SplitFundName(i.FileRow.FundName).FundName,
        ShortName = SplitFundName(i.FileRow.FundName).FundName.Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        CountryId = i.Country.Id,
        DomicileId = i.Country.Id,
        SicavId = i.Sicav?.Id,
        PricingFrequency = FrequencyType.Daily,
        CutOffTime = TimeSpan.TryParse("14:00",out var res)?res: (TimeSpan?) null,
        SettlementNbDays = 3,
    })
    .EfCoreSave($"{TaskName}: Save SubFund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());
#endregion

#region SHARE CLASSES (INTERNAL)
var shareClassStream = navFileStream
    .LookupCurrency($"{TaskName}: Get related currency for shareclass", l => l.Currency, (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: Get related sub fund", subfundsStream, (l, r) => new { l.Currency, l.FileRow, SubFund = r })
    .Select($"{TaskName}: Create share classes", i => new {
        i.FileRow.TotalNetAsset,
        ShareClass=new ShareClass
        {
            InternalCode = i.FileRow.IsinCode,
            Name = $"{SplitFundName(i.FileRow.FundName).FundName} - {i.FileRow.NameOfShares} {i.FileRow.Currency}",
            ShortName = $"{i.FileRow.NameOfShares} {i.FileRow.Currency}".Truncate(MaxLengths.ShortName),
            CurrencyId = i.Currency?.Id,
            SubFundId = i.SubFund!=null? i.SubFund.Id: throw new Exception($"Sub fund not found for share class: {i.FileRow.NameOfShares} {i.FileRow.Currency}"),
            Isin = i.FileRow.IsinCode,
            InvestorType = i.FileRow.NameOfShares.Contains("I")? InvestorType.Institutional
                            : i.FileRow.NameOfShares.Contains("R")? InvestorType.Retail 
                            : i.FileRow.NameOfShares.Contains("P")? InvestorType.Retail 
                            : (InvestorType?) null,
            DividendDistributionPolicy  = i.FileRow.NameOfShares.Contains("D")? DividendDistributionPolicy.Distribution : 
                                            DividendDistributionPolicy.Accumulation,
        }})
    .Distinct($"{TaskName}: Distinct shareclass", i => i.ShareClass.InternalCode)
    .EfCoreSave($"{TaskName}: Insert share classes", o => o
        .Entity(i=>i.ShareClass)
        .SeekOn(i => i.Isin)
        .AlternativelySeekOn(i => i.InternalCode)
        .Output((i,j)=> i)
        .DoNotUpdateIfExists());

var primaryShareClassStream=shareClassStream
        .Sort($"{TaskName}: Sort share classes by AUM desc", i => new {i.ShareClass.SubFundId,i.TotalNetAsset}, new {FundCode = 1,TotalNetAsset = -2})
        .Distinct($"{TaskName}: Distinct AUM sorted share classes", i => i.ShareClass.SubFundId); //take the shareclass id that comes first at it has the biggest aum for the subfund

var subfundsPrimaryShareClassStream = subfundsStream
    .CorrelateToSingle($"{TaskName}: get related primary share class", primaryShareClassStream,(sf, sc)=> {
        sf.PrimaryShareClassId = sc?.ShareClass?.Id;
        return sf;
    })
    .EfCoreSave($"{TaskName}: save sf");

#endregion

#region HISTORICAL VALUES
// Portfolio HV
var savedPortfolioHistoricalValueStream = navFileStream
    .CorrelateToSingle($"{TaskName}: Get related portfolio for HV", subfundsStream, (l, r) => new { l.FundTotalNetAsset, l.NbShares, l.QuantitySubscription, l.QuantityRedemption, l.NavDate, PortfolioId = r.Id })
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
    .CorrelateToSingle($"{TaskName}: Get related shareclass for HV", shareClassStream, (l, r) => new { FileRow = l, ShareClass = r.ShareClass })
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
#endregion

return FileStream.WaitWhenDone($"{TaskName}: Wait till everything is saved", savedPortfolioHistoricalValueStream, savedShareClassHistoricalValueStream,
            subfundsPrimaryShareClassStream);

#region Helpers
(string FundName, string SicavName) SplitFundName(string name)
{
    var idx = name.IndexOf("-");
    if (idx < 0) return (name, (string)null);
    return (name.Substring(idx + 1).Trim(), name.Substring(0, idx).Trim());
}
#endregion
