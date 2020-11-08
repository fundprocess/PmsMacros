var kbcPositionFileDefinition = FlatFileDefinition.Create(i => new
{
    ClientNbr = i.ToColumn<string>("CLIENT NBR"),
    Isin = i.ToColumn<string>("ISIN"),
    SecNbr = i.ToColumn<string>("SEC NBR"),
    SecName = i.ToColumn<string>("SEC NAME"),
    SecCur = i.ToColumn<string>("SEC CUR"),
    Position = i.ToNumberColumn<double>("POSITION", "."),
    Value = i.ToNumberColumn<double?>("VALUE", "."),
    ValueSecIntIn = i.ToNumberColumn<double?>("VALUE SEC INT IN", "."),
    ValueSecIntEx = i.ToNumberColumn<double?>("VALUE SEC INT EX", "."),
    PosDate = i.ToOptionalDateColumn("POS_DATE", "yyyyMMdd"),
    ValPortIntInc = i.ToNumberColumn<double?>("VAL PORT INT INC", "."),
    SecType = i.ToColumn<string>("SEC TYPE"),
    FileName = i.ToSourceName()
}).IsColumnSeparated(',');


var euroCurrencyStream = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", i => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse position file", kbcPositionFileDefinition)
    .Fix($"{TaskName}: Parse Date", o => o
        .FixProperty(p => p.PosDate).IfNullWith(i => (!DateTime.TryParseExact(i.FileName.Substring(0, 10), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date) ? (DateTime?)null : date)))
    .Where($"{TaskName}: exclude entries without date", i => i.PosDate != null)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var portfolioStream = posFileStream
    .Distinct($"{TaskName}: distinct portfolio", i => i.ClientNbr)
    .Select($"{TaskName}: create portfolio with euro currency id", euroCurrencyStream, (l, r) => new SubFund
    {
        InternalCode = l.ClientNbr,
        Name = $"KBC_{l.ClientNbr}",
        ShortName = $"KBC_{l.ClientNbr}".Truncate(MaxLengths.ShortName),
        CurrencyId = r.Id
    })
    .EfCoreSave($"{TaskName}: save portfolio", o => o.SeekOn(i => i.InternalCode));

var targetSecurityStream = posFileStream
    .ReKey($"{TaskName}: Uniformize target instrument codes", i => new { i.Isin, i.SecNbr })
    .Distinct($"{TaskName}: distinct target security", i => new { i.Isin, i.SecNbr })
    .LookupCurrency($"{TaskName}: get related currency for target security", l => l.FileRow.SecCur, (l, r) => new
    {
        l.FileRow.SecNbr,
        l.FileRow.SecType,
        l.FileRow.SecName,
        l.FileRow.Isin,
        CurrencyId = r?.Id
    })
    .Select($"{TaskName}: create security for composition", i => CreateSecurityForComposition(i.SecNbr, i.SecType, i.SecName, i.CurrencyId, i.Isin));

var targetCashStream = targetSecurityStream
    .Select($"{TaskName}: cast to cash", i => i as Cash)
    .WhereCorrelated($"{TaskName}: keep cash types", i => i != null)
    .EfCoreSave($"{TaskName}: save target cash by internal code", o => o.SeekOn(i => i.InternalCode));

var targetInstrumentStream = targetSecurityStream
    .Select($"{TaskName}: cast to security instruments", i => i as SecurityInstrument)
    .WhereCorrelated($"{TaskName}: keep known security instrument types", i => i != null)
    .EfCoreSave($"{TaskName}: save target instrument by isin, then by internal code", o => o.SeekOn(i => i.Isin).AlternativelySeekOn(i => i.InternalCode));

// targetSecurityStream = targetCashStream.Select($"{TaskName}: cast cash back to Security", i => i as Security)
//     .Union($"{TaskName}: put cash and instrument together", targetInstrumentStream.Select($"{TaskName}: cast instrument back to Security", i => i as Security));
targetSecurityStream = targetCashStream
    .Union($"{TaskName}: put cash and instrument together", targetInstrumentStream, i => i as Security, i => i as Security);

var portfolioCompositionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => new PortfolioComposition { Date = l.PosDate.Value, PortfolioId = r.Id })
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioId, i.Date }, true)
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));




var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r) => new { l.FileRow, l.Security, Composition = r })
    .Aggregate($"{TaskName}: sum positions duplicates within a file",
        i => new
        {
            i.FileRow.FileName,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id
        },
        i => new
        {
            i.FileRow,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id,
            Values = new
            {
                Position = (double)0,
                ValPortIntInc = (double)0,
                Value = (double?)null,
                ValueSecIntEx = (double)0,
                ValueSecIntIn = (double)0,
                Count = 0
            }
        },
        (a, v) =>
        new
        {
            v.FileRow,
            CompositionId = v.Composition.Id,
            SecurityId = v.Security.Id,
            Values = new
            {
                Position = a.Values.Position + v.FileRow.Position,
                ValPortIntInc = a.Values.ValPortIntInc + v.FileRow.ValPortIntInc ?? 0,
                Value = (a.Values.Value == null && v.FileRow.Value == null) ? (double?)null : (a.Values.Value ?? 0) + (v.FileRow.Value ?? 0),
                ValueSecIntEx = a.Values.ValueSecIntEx + v.FileRow.ValueSecIntEx ?? 0,
                ValueSecIntIn = a.Values.ValueSecIntIn + v.FileRow.ValueSecIntIn ?? 0,
                Count = a.Values.Count + 1
            }
        })
    .Select($"{TaskName}: get position aggregation",
        i =>
        new
        {
            CompositionId = i.Aggregation.CompositionId,
            SecurityId = i.Aggregation.SecurityId,
            Values = new
            {
                Position = i.Aggregation.Values.Position,
                ValPortIntInc = i.Aggregation.Values.ValPortIntInc,
                Value = i.Aggregation.Values.Value,
                ValueSecIntEx = i.Aggregation.Values.ValueSecIntEx,
                ValueSecIntIn = i.Aggregation.Values.ValueSecIntIn,
            }
        })
    .Distinct($"{TaskName}: exclude positions duplicates", i => new { i.CompositionId, i.SecurityId }, true)
    .Select($"{TaskName}: create position", i => new Position
    {
        SecurityId = i.SecurityId,
        PortfolioCompositionId = i.CompositionId,
        MarketValueInPortfolioCcy = i.Values.Value ?? i.Values.ValPortIntInc,
        MarketValueInSecurityCcy = i.Values.ValueSecIntIn,
        Value = i.Values.Position
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));
return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream);
Security CreateSecurityForComposition(string secNbr, string secType, string secName, int? currencyId, string isin)
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
        security.InternalCode = secNbr;
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
