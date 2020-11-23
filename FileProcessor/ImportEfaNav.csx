var efaNavFileDefinition = FlatFileDefinition.Create(i => new
{
    ShareCode = i.ToColumn<string>("Share_code"),
    SubFundCode = i.ToColumn<string>("Sub-fund_code"),
    IsinCode = i.ToColumn<string>("Isin_code"),
    ShareCurrency = i.ToColumn<string>("CCY_NAV_share"),
    NavDate = i.ToDateColumn("Valuation_date", "dd/MM/yyyy"),
    NavPerShare = i.ToNumberColumn<double>("NAV_share", "."),
    NumberOfSharesOutstanding = i.ToNumberColumn<double>("Numbre_of_Shares_outstanding", "."),
    TotalNetAsset = i.ToNumberColumn<double>("Total_Net_Assets", "."),
    NetAssetShareType = i.ToNumberColumn<double>("Net_assets_share_type", "."),
}).IsColumnSeparated(',');

var navFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse nav file", efaNavFileDefinition)
    .SetForCorrelation($"{TaskName}: prepare correlation");

var managedSubFundStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get subfunds from db", (ctx, j) => ctx.Set<SubFund>());

var managedShareClassStream = navFileStream
    .Distinct($"{TaskName}: distinct share classes", i => new { i.SubFundCode, i.ShareCode }, true)
    .LookupCurrency($"{TaskName}: get related currency for share class", l => l.ShareCurrency, (l, r) => new { l.SubFundCode, l.ShareCode, l.IsinCode, CurrencyId = r?.Id })
    .Lookup($"{TaskName}: lookup related sub fund", managedSubFundStream, i => i.SubFundCode, i => i.InternalCode, (l, r) => new { FromFile = l, SubFund = r })
    .Select($"{TaskName}: create share class", i => new ShareClass
    {
        InternalCode = $"{i.FromFile.SubFundCode}_{i.FromFile.ShareCode}",
        // InternalCode = i.FromFile.IsinCode, //$"{i.SubFund.InternalCode}_{i.FromFile.ShareCode}",
        Name = $"{i.SubFund.Name} {i.FromFile.ShareCode}",
        ShortName = $"{i.SubFund.Name} {i.FromFile.ShareCode}",
        CurrencyId = i.FromFile.CurrencyId,
        Isin = i.FromFile.IsinCode,
        SubFundId = i.SubFund.Id
    })
    .EfCoreSave($"{TaskName}: save share class", o => o.SeekOn(i => i.InternalCode));

#region Historical values

var savedShareClassHvStream = navFileStream
    .Distinct($"{TaskName}: distinct nav per shareclass", i => new { i.NavDate, i.SubFundCode, i.ShareCode }, true)
    .CrossApplyEnumerable($"{TaskName}: unpivot share class historical values", i => new[]{
        new { Date = i.NavDate, ShareClassInternalCode = $"{i.SubFundCode}_{i.ShareCode}", Type = HistoricalValueType.TNA, Value = i.NetAssetShareType },
        new { Date = i.NavDate, ShareClassInternalCode = $"{i.SubFundCode}_{i.ShareCode}", Type = HistoricalValueType.MKT, Value = i.NavPerShare },
        new { Date = i.NavDate, ShareClassInternalCode = $"{i.SubFundCode}_{i.ShareCode}", Type = HistoricalValueType.NBS, Value = i.NumberOfSharesOutstanding },
    })
    .CorrelateToSingle($"{TaskName}: get hv related share class", managedShareClassStream, (l, r) => new { FromFile = l, FromDb = r })
    .Select($"{TaskName}: create share class hv", i => new SecurityHistoricalValue
    {
        SecurityId = i.FromDb.Id,
        Date = i.FromFile.Date,
        Type = i.FromFile.Type,
        Value = i.FromFile.Value
    })
    .EfCoreSave($"{TaskName}: save share class hv", o => o.SeekOn(i => new { i.Date, i.SecurityId, i.Type }));

var savedSubFundHvStream = navFileStream
    .Distinct($"{TaskName}: distinct nav per subfund", i => new { i.NavDate, i.SubFundCode }, true)
    .CrossApplyEnumerable($"{TaskName}: unpivot subfund historical values", i => new[]{
        new { SubFundInternalCode = i.SubFundCode, Date = i.NavDate, Type = HistoricalValueType.TNA, Value = i.TotalNetAsset }
    })
    .Lookup($"{TaskName}: get hv related subfund", managedSubFundStream, i => i.SubFundInternalCode, i => i.InternalCode, (l, r) => new { FromFile = l, FromDb = r })
    .Select($"{TaskName}: create sub fund hv", i => new PortfolioHistoricalValue
    {
        PortfolioId = i.FromDb.Id,
        Date = i.FromFile.Date,
        Type = i.FromFile.Type,
        Value = i.FromFile.Value
    })
    .EfCoreSave($"{TaskName}: save subfund hv", o => o.SeekOn(i => new { i.Date, i.PortfolioId, i.Type }));

#endregion
return FileStream.WaitWhenDone($"{TaskName}: wait till every hv is saved", savedShareClassHvStream, savedSubFundHvStream);
