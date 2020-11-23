//1. FILE COLUMN MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn("CLIENT NBR"),
    PortfolioCcy = i.ToColumn("NET CUR"),
    SecurityName = i.ToColumn("SEC NAME"),
    SecurityCode = i.ToColumn("SEC NBR"),
    SecurityCcy = i.ToColumn("SEC CUR"),
    SecurityIsin = i.ToColumn("ISIN"),
    Canceled = i.ToBooleanColumn("CANC", "Y", "N"),
    TransactionCodeRefExtern = i.ToColumn("REF EXTERN"),
    TransactionCodeTrnCode = i.ToColumn("TRN CODE"),
    Description = i.ToColumn("TRN DESC"),
    TradeDate = i.ToDateColumn("TRADING DATE", "yyyyMMdd"),
    //NavDate = i.ToDateColumn("VALUE DATE", "yyyyMMdd"),
    ValueDate = i.ToDateColumn("VALUE DATE", "yyyyMMdd"),
    Quantity = i.ToNumberColumn<double>("QUANTITY", "."),
    OperationType = i.ToColumn("D/C"),
    //FeesInSecurityCcy = i.ToNumberColumn(""),
    //GrossAmountInPortfolioCcy = i.ToColumn(""),
    GrossAmountInSecurityCcy = i.ToNumberColumn<double?>("PAYM GROSS", "."),
    NetAmountInPortfolioCcy = i.ToNumberColumn<double?>("NET AMOUNT", "."),
    //NetAmountInSecurityCcy = i.ToColumn(""),
    PriceInSecurityCcy = i.ToNumberColumn<double?>("PRICE", "."),
    FxRate = i.ToNumberColumn<double>("EXCH RATE", "."),
}).IsColumnSeparated(',');

var transFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//2. CREATE PORTFOLIOS
// Portfolio
var portfolioStream = transFileStream
    .LookupCurrency($"{TaskName}: Get related currency for portfolio", l => l.PortfolioCcy, (l, r) => new { FileRow = l, Currency = r })
    .Select($"{TaskName}: Create portfolios", ProcessContextStream, (i, ctx) => new DiscretionaryPortfolio
    {
        InternalCode = i.FileRow.PortfolioCode,
        Name = i.FileRow.PortfolioCode,
        ShortName = "KBC",
        CurrencyId = i.Currency?.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .Distinct($"{TaskName}: Distinct portfolios", i => i.InternalCode)
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//Assign "discretionary" classification on portfolios
// 1. Type definition
var discretionaryPortfolioTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create internal portfolios classification type", ctx => new SecurityClassificationType
    {
        Code = "InternalPortfolioClassificationType",
        Name = new MultiCultureString { ["en"] = "Internal Portfolio Classification" },
        Description = new MultiCultureString { ["en"] = "Internal Portfolio Classification" }
    })
    .EfCoreSave($"{TaskName}: Savediscretionary portfolios type", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: ensure single InternalPortfolioClassificationType");

// 2. Classification definition
var discretionaryClassificationStream = discretionaryPortfolioTypeStream
    .Select($"{TaskName}: DiscretionaryPortfolioClassification", i => new SecurityClassification
    {
        Code = "DiscretionaryPortfolio",
        Name = new MultiCultureString { ["en"] = "Discretionary Portfolio", ["en"] = "Portefeuille discretionnaire" },
        ClassificationTypeId = i.Id
    })
    .EfCoreSave($"{TaskName}: Save Portfolio Classification ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: ensure single discretionary classification");

// 3. Classification assignation
var discretionaryPortfolioAssignations = portfolioStream
        .Select($"{TaskName}: Assign discretionary classification on portfolio", discretionaryClassificationStream,
        (port, c) => new ClassificationOfPortfolio { ClassificationTypeId = c.ClassificationTypeId, PortfolioId = port.Id, ClassificationId = c.Id })
        .EfCoreSave($"{TaskName}: Save classification assignation ", o => o.SeekOn(c => new { c.PortfolioId, c.ClassificationId }).DoNotUpdateIfExists());


//2. CREATE TARGET SECURITIES
var targetInstrumentStream = transFileStream
    .Distinct($"{TaskName}: distinct position securities", i => i.SecurityCode)
    .LookupCurrency($"{TaskName}: get related currency", l => l.SecurityCcy, (l, r) => new { FileRow = l, Currency = r })
    //CreateSecurity(string securityCode, string secName,string description, string isin, int? currencyId)
    .Select($"{TaskName}: create target security", i => CreateSecurity(i.FileRow.SecurityCode, i.FileRow.SecurityName, i.FileRow.Description,
                i.FileRow.SecurityIsin, i.Currency?.Id) as SecurityInstrument)
    .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).AlternativelySeekOn(i => i.Isin).DoNotUpdateIfExists());
//.Select($"{TaskName}: cast instrument into Security", i => i as Security);

//3. CREATE TRANSACTIONS
var savedTransactionsStream = transFileStream
    .LookupPortfolio($"{TaskName}: Lookup for portfolio", i => i.PortfolioCode, (l, r) => new { FileRow = l, Portfolio = r })
    .EfCoreLookup($"{TaskName}: get target security by internalcode", o => o
        .Set<SecurityInstrument>().On(i => (!string.IsNullOrEmpty(i.FileRow.SecurityIsin)) ? i.FileRow.SecurityIsin : i.FileRow.SecurityCode, i => i.InternalCode)
        .Select((l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r }))
    .Where($"{TaskName}: Exclude transactions with unfound portfolio", i => i.Portfolio != null)
    .Where($"{TaskName}: Exclude transactions with target security not found", i => i.TargetSecurity != null)
    .Where($"{TaskName}: Exclude canceled", i => !i.FileRow.Canceled)
    .Select($"{TaskName}: Create new security transaction", i => new SecurityTransaction
    {
        PortfolioId = i.Portfolio.Id,
        SecurityId = i.TargetSecurity.Id,
        OperationType = i.FileRow.OperationType == "C" ? OperationType.Buy : OperationType.Sale,
        TransactionCode = i.FileRow.TransactionCodeRefExtern,
        Description = i.FileRow.Description + "-" + i.FileRow.TransactionCodeTrnCode,
        TradeDate = i.FileRow.TradeDate,
        NavDate = i.FileRow.TradeDate,
        ValueDate = i.FileRow.ValueDate,
        Quantity = i.FileRow.Quantity,

        GrossAmountInSecurityCcy = i.FileRow.GrossAmountInSecurityCcy.Value,
        GrossAmountInPortfolioCcy = (i.FileRow.GrossAmountInSecurityCcy.Value / i.FileRow.FxRate),

        NetAmountInPortfolioCcy = i.FileRow.NetAmountInPortfolioCcy,
        NetAmountInSecurityCcy = (i.FileRow.NetAmountInPortfolioCcy * i.FileRow.FxRate),

        FeesInSecurityCcy = i.FileRow.GrossAmountInSecurityCcy.Value - (i.FileRow.NetAmountInPortfolioCcy * i.FileRow.FxRate),

        PriceInSecurityCcy = i.FileRow.PriceInSecurityCcy.Value,
        TransactionType = TransactionType.SecurityMovement,
        DecisionType = TransactionDecisionType.Discretionary,
    }).EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedTransactionsStream);

Security CreateSecurity(string securityCode, string secName, string description, string isin, int? currencyId)
{
    Security security = null;
    if (description.ToLower().Contains("fund"))
        security = new ShareClass();
    else if (!string.IsNullOrEmpty(isin) && (secName.Contains("%")))
        security = new Bond();
    else if (!string.IsNullOrEmpty(isin))
        security = new Equity();
    else
        throw new Exception("Unknow security type: " + secName);

    security.InternalCode = !string.IsNullOrEmpty(isin) ? isin : securityCode;
    security.CurrencyId = currencyId;
    security.Name = secName;
    security.ShortName = security.Name.Truncate(MaxLengths.ShortName);

    if (security is SecurityInstrument securityInstrument)
        securityInstrument.Isin = isin;

    if (security is RegularSecurity regularSecurity)
        regularSecurity.PricingFrequency = FrequencyType.Daily;

    return security;
}