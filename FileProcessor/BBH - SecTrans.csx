string GetPortfolioCode()
    => "ValuFocus-BBH";
//!! Missing columns: FundCode, SecCode

//MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    TransactionReferenceID = i.ToColumn("Transaction Reference ID"),
    TransactionStatusDescription = i.ToColumn("Transaction Status Description"),
    //InstructionDate = i.ToDateColumn("Instruction Date", "yyyy-MM-dd"),
    //InstructionTime = i.ToColumn("Instruction Time"),
    TradeDate = i.ToDateColumn("Trade Date", "yyyy-MM-dd HH:mm:ss"),
    SecurityDescription = i.ToColumn("Security Description"),
    Isin = i.ToColumn("ISIN"),
    UnitsOfQuantity = i.ToNumberColumn<double?>("Units of Quantity", "."),
    UnitPrice = i.ToNumberColumn<double?>("Unit Price", "."),
    TransactionCurrency = i.ToColumn("Transaction Currency"),
    CashAmount = i.ToNumberColumn<double?>("Cash Amount", "."),
    SettlementDate = i.ToDateColumn("Settlement Date", "yyyy-MM-dd HH:mm:ss"), //2020-12-08 00:00:00
    TradingBroker = i.ToColumn("Trading Broker"),
}).IsColumnSeparated(',');

var transFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

#region broker and counterparties
var brokerCompanyStream = transFileStream
    .Distinct($"{TaskName}: exclude duplicate brokers", i => i.TradingBroker)
    .Select($"{TaskName}: create broker companies", i => new Company
    {
        InternalCode = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.TradingBroker.ToLower()).Replace(" ", ""),
        Name = i.TradingBroker
    }).EfCoreSave($"{TaskName}: save broker companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var counterpartyStream = brokerCompanyStream
    .Select($"{TaskName}: create counterparty", euroCurrency, (i, j) => new CounterpartyRelationship
    {
        EntityId = i.Id,
        StartDate = DateTime.Today,
        CounterpartyType = CounterpartyType.Broker,
        CurrencyId = j.Id,
        EmirClassification = EmirClassification.Financial,
    }).EfCoreSave($"{TaskName}: save counterparty", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());
#endregion broker and counterparties

var subFundsStream = ProcessContextStream.CrossApplyEnumerable($"{TaskName}: Cross-apply Sub Fund",ctx=>
    new [] {
        new { InternalCode = GetPortfolioCode(), Name = GetPortfolioCode(), Currency="USD"},
    })
    .LookupCurrency($"{TaskName}: Create Sub Fund Ccy", i => i.Currency,
        (l,r) => new {Row = l, Currency = r})
    .Select($"{TaskName}: Create Sub Fund",i => new SubFund{
        InternalCode = i.Row.InternalCode,
        Name = i.Row.Name,
        ShortName = i.Row.InternalCode,
        CurrencyId = i.Currency.Id,
    })
    .EfCoreSave($"{TaskName}: Save Sub Fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: ensures only one sub fund");

// Security Description	ISIN	Units of Quantity	Unit Price	Transaction Currency
// SANDVIK AB /SEK/	SE0000667891	9727	190.9681	SEK
// .EfCoreLookup($"{TaskName}: get target security by internalcode", o => o
//         .Set<SecurityInstrument>().On(i => (!string.IsNullOrEmpty(i.FileRow.SecurityIsin))?i.FileRow.SecurityIsin:i.FileRow.SecurityCode 
//             , i => i.InternalCode)
//         .Select((l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r }).CacheFullDataset())


var savedTransactionStream = transFileStream
    .EfCoreLookup($"{TaskName}: get related target security", o => o.Set<SecurityInstrument>().On(i => i.Isin, i=> i.Isin)
        .Select((l, r) => new { FileRow = l, TargetSecurity = r }).CacheFullDataset())
    .CorrelateToSingle($"{TaskName}: get broker by internal code", counterpartyStream, 
        (l, r) => new { l.FileRow, l.TargetSecurity, Broker = r })
    //.Where($"{TaskName}: exclude tra with target security not found", i => i.TargetSecurity != null)
    .Select($"{TaskName}: Create security transaction",subFundStream, (i,j) => new SecurityTransaction
    {
        PortfolioId = j.Id,
        SecurityId = (i.TargetSecurity != null)? i.TargetSecurity.Id : throw new Exception("Security not found: "+i.FileRow.Isin),
        OperationType = i.FileRow.UnitsOfQuantity > 0? OperationType.Buy : OperationType.Sale,
        TransactionCode = $"{i.FileRow.TransactionReferenceID}-BBH",
        Description = i.FileRow.TransactionStatusDescription,
        TradeDate = i.FileRow.TradeDate,
        NavDate = i.FileRow.TradeDate,
        ValueDate = i.FileRow.SettlementDate,
        Quantity = (i.FileRow.UnitsOfQuantity.HasValue)? i.FileRow.UnitsOfQuantity.Value 
                        : throw new Exception("Quantity not provided: "+i.FileRow.TransactionReferenceID),
        // FeesInSecurityCcy = i.FileRow.FeesInSecurityCcy,
        // // //GrossAmountInPortfolioCcy = ...
        // GrossAmountInSecurityCcy = i.FileRow.GrossAmountInSecurityCcy.Value,
        // // //NetAmountInPortfolioCcy = ...
        // NetAmountInSecurityCcy = i.FileRow.NetAmountInSecurityCcy,
        PriceInSecurityCcy = (i.FileRow.UnitPrice.HasValue)? i.FileRow.UnitPrice.Value 
                        : throw new Exception("UnitPrice not provided: "+i.FileRow.TransactionReferenceID),
        BrokerId = i.Broker.Id,
        TransactionType = TransactionType.SecurityMovement,
        DecisionType = TransactionDecisionType.Discretionary,
    }).EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved",subFundsStream, savedTransactionStream);

