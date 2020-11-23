//"Fund-ID";"Valuation";"Monthly";"Currency";"Subscription price";"Redemption price";"Outstanding shares";
//"Short ID";"Share value";"NAV";"BVI-Factor";"Cum. BVI-Factor";"Performance";"Benchmark Performance"

var navFileDefinition = FlatFileDefinition.Create(i => new
{
    ShareCode = i.ToColumn<string>("Fund-ID"),
    ShareName = i.ToColumn<string>("Short ID"),
    ShareCurrency = i.ToColumn<string>("Currency"),
    NavDate = i.ToDateColumn("Valuation", "dd.MM.yyyy"),
    NavPerShare = i.ToNumberColumn<double?>("Share value", ","),
    NumberOfSharesOutstanding = i.ToNumberColumn<double?>("Outstanding shares", ","),
    TotalNetAsset = i.ToNumberColumn<double?>("NAV", ","),
}).IsColumnSeparated(';');

var navFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse nav file", navFileDefinition)
    .SetForCorrelation($"{TaskName}: prepare correlation")
    // .Fix($"{TaskName}Fix some columns",o=>o
    //     .FixProperty(i=>i.SubFundCode).IfNotNullWith(i=>i.SubFundCode.Substring(0,4))
    //     .FixProperty(i=>i.ShareShortName).IfNotNullWith(i=>i.ShareShortName.Substring(4,2))
    //     )
    .Where($"{TaskName}: keep valid lines only", i => !string.IsNullOrEmpty(i.ShareCode));

var managedSubFundStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get subfunds from db", (ctx, j) => ctx.Set<SubFund>());

var managedShareClassStream = navFileStream
    .Distinct($"{TaskName}: distinct share classes", i => new { i.ShareCode }, true)
    .LookupCurrency($"{TaskName}: get related currency for share class", l => l.ShareCurrency,
                            (l, r) => new { l.ShareCode, l.ShareName, CurrencyId = r?.Id })
    .Lookup($"{TaskName}: lookup related sub fund", managedSubFundStream, i => i.ShareCode.Substring(0, 4), i => i.InternalCode,
                            (l, r) => new { FileRow = l, SubFund = r })
    .Select($"{TaskName}: create share class", i => new ShareClass
    {
        InternalCode = i.FileRow.ShareCode,
        Name = i.FileRow.ShareName,
        ShortName = i.FileRow.ShareCode.Substring(4, 2),
        CurrencyId = i.FileRow.CurrencyId,
        Isin = i.FileRow.ShareCode,
        SubFundId = i.SubFund.Id
    })
    .EfCoreSave($"{TaskName}: save share class", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//SubFundCode = i.ShareCode.Substring(0,4)
var savedShareClassHvStream = navFileStream
    .Distinct($"{TaskName}: distinct nav per shareclass", i => new { i.NavDate, i.ShareCode }, true)
    .CrossApplyEnumerable($"{TaskName}: unpivot share class historical values", i => new[]{
        new { Date = i.NavDate, ShareClassInternalCode =i.ShareCode, Type = HistoricalValueType.TNA, Value = i.TotalNetAsset },
        new { Date = i.NavDate, ShareClassInternalCode =i.ShareCode, Type = HistoricalValueType.MKT, Value = i.NavPerShare },
        new { Date = i.NavDate, ShareClassInternalCode =i.ShareCode, Type = HistoricalValueType.NBS, Value = i.NumberOfSharesOutstanding },
    })
    .CorrelateToSingle($"{TaskName}: get hv related share class", managedShareClassStream, (l, r) => new { FromFile = l, FromDb = r })
    .Where($"{TaskName}: Exclude empty hv", i => i.FromFile.Value.HasValue)
    .Select($"{TaskName}: create share class hv", i => new SecurityHistoricalValue
    {
        SecurityId = i.FromDb.Id,
        Date = i.FromFile.Date,
        Type = i.FromFile.Type,
        Value = i.FromFile.Value.Value
    })
    .EfCoreSave($"{TaskName}: save share class hv", o => o.SeekOn(i => new { i.Date, i.SecurityId, i.Type }));

return FileStream.WaitWhenDone($"{TaskName}: wait till every hv is saved", savedShareClassHvStream);

