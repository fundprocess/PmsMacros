var tradeBookFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("PortfolioCode"),
    Date = i.ToDateColumn("Date", "yyyyMMdd"),
    SecurityCode = i.ToColumn<string>("SecurityCode"),
    Isin = i.ToColumn<string>("ISIN"),
    BuySell = i.ToColumn<string>("BuySell"),
    Quantity = i.ToNumberColumn<double?>("Quantity", "."),
    AmountInPtfCcy = i.ToNumberColumn<double?>("AmountInPtfCcy", "."),
    PlacedBy = i.ToColumn<string>("PlacedBy")
}).IsColumnSeparated(',');

var tradeFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse tradebook file", tradeBookFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");


var trades = tradeFileStream.Select(i=> new Trade() );


var tstClassificationType = ProcessContextStream.EfCoreSelect("Get tst classification type", i => i
    .Set<SecurityClassificationType>()
    .Where(ct => ct.Code == ClassificationType))
    .EnsureSingle($"Ensure only one {ClassificationType} classification type exists");

var classificationsOfSecurity = ProcessContextStream.EfCoreSelect("Get every security", i => i
    .Set<SecurityInstrument>()
    .Where(s => !s.Classifications.Any(c => c.ClassificationType.Code == ClassificationType)))
    .Select("Compute the classification of the security", tstClassificationType, (s, t) => new
    {
        SecurityId = s.Id,
        ClassificationCode = GetClassification(s),
        ClassificationTypeId = t.Id
    })
    .EfCoreLookup("Get related classification Id", o => o.LeftJoinEntity(i => i.ClassificationCode, (SecurityClassification sc) => sc.Code, (l, r) => new ClassificationOfSecurity
    {
        SecurityId = l.SecurityId,
        ClassificationId = r.Id,
        ClassificationTypeId = l.ClassificationTypeId
    }))
    .EfCoreSave("Save security classifications");


//  public class Trade : IMultiTenantEntity, IId
//     {
//         public int Id { get; set; }
//         public int PortfolioId { get; set; }
//         public Portfolio Portfolio { get; set; }
//         public DateTime Date { get; set; }
//         public int Position { get; set; }
//         public string Isin { get; set; }
//         public int? SecurityId { get; set; }
//         public Security Security { get; set; }
//         public BuySell BuySell { get; set; }
//         public double? Quantity { get; set; }
//         public double? AmountInPtfCcy { get; set; }
//         public int PlacedById { get; set; }
//         public RoleRelationship PlacedBy { get; set; }
//         public int TenantId { get; set; }
//     }






var targetSecurityStream = posFileStream
    .ReKey($"{TaskName}: Uniformize target instrument codes", i => new { i.Isin, i.SecNbr }, (i, k) => new { FileRow = i, Key = k })
    .Distinct($"{TaskName}: distinct target security", i => i.Key)
    .LookupCurrency($"{TaskName}: get related currency for target security", l => l.FileRow.SecCur, (l, r) => new
    {
        l.Key.SecNbr,
        l.FileRow.SecType,
        l.FileRow.SecName,
        l.Key.Isin,
        CurrencyId = r?.Id
    })
    .Select($"{TaskName}: create security for composition", i => CreateSecurityForComposition(i.SecNbr, i.SecType, i.SecName, i.CurrencyId, i.Isin));


return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream);

Trade CreateTrade(string secNbr, string secType, string secName, int? currencyId, string isin)
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
