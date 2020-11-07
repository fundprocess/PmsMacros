//MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn("FundCode"),
    //SecurityCode = i.ToColumn("Isin"),
    IsinCode = i.ToColumn("Isin"),
    //TransactionCode = ,
    Description = i.ToColumn("Security Desc"),
    TradeDate = i.ToDateColumn("Trade Date", "yyyy/MM/dd"),
    //NavDate = i.ToDateColumn("Instruction Date", "yyyy/MM/dd"),
    ValueDate = i.ToDateColumn("Settle Date", "yyyy/MM/dd"),
    Quantity = i.ToNumberColumn<double?>("Units", "."),
    OperationType = i.ToColumn("Type"),//RVP= Receive vs Payment = Purchase/DVP= Delivery vs Payment = Sale
    FeesInSecurityCcy = i.ToNumberColumn<double?>("Commission", "."),
    //GrossAmountInPortfolioCcy = i.ToNumberColumn<double?>("Amount", "."),
    GrossAmountInSecurityCcy = i.ToNumberColumn<double?>("Amount", "."),
    //NetAmountInPortfolioCcy
    NetAmountInSecurityCcy = i.ToNumberColumn<double?>("Principal", "."),
    PriceInSecurityCcy = i.ToNumberColumn<double?>("Unit Price", "."),
    BrokerInternalCode = i.ToColumn("Trading Broker"), 
    Status = i.ToColumn("Trans Status"), 
    //TransactionType TransactionType //(SecurityMovement/SpotExchange)
    //DecisionType //Discretionary vs Signal
}).IsColumnSeparated(',');

var transFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var brokerCompanyStream = transFileStream
    .Distinct($"{TaskName}: exclude duplicate brokers", i => i.BrokerInternalCode)
    .Select($"{TaskName}: create broker companies", i => new Company
    {
        InternalCode = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.BrokerInternalCode.ToLower()).Replace(" ",""),
        Name = i.BrokerInternalCode
    }).EfCoreSave($"{TaskName}: save broker companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", i => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var counterpartyStream = brokerCompanyStream
    .Select($"{TaskName}: create counterparty",euroCurrency, (i,j) => new CounterpartyRelationship
    {
        EntityId = i.Id, 
        StartDate = DateTime.Today,
        CounterpartyType = CounterpartyType.Broker,
        CurrencyId = j.Id,
        EmirClassification = EmirClassification.Financial,
    }).EfCoreSave($"{TaskName}: save counterparty", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());

var savedTransactionStream = transFileStream
    .LookupPortfolio($"{TaskName}: Lookup for portfolio", i => i.PortfolioCode, (l, r) => new { FileRow = l, Portfolio = r })    
    .EfCoreLookup($"{TaskName}: get target security by isin", o => o
        .Set<SecurityInstrument>().On(i => i.FileRow.IsinCode, i => i.Isin)
        .Select((l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r }).CacheFullDataset())
    .CorrelateToSingle($"{TaskName}: get broker by internal code", counterpartyStream, (l, r) => new { l.FileRow, l.Portfolio, l.TargetSecurity, Broker = r })
    .Where($"{TaskName}: exclude transaction with unfound portfolio", i => i.Portfolio != null)
    .Where($"{TaskName}: exclude movements with target security not found", i => i.TargetSecurity != null)
    .Where($"{TaskName}: Exclude Status = CN", i=>i.FileRow.Status!="CN")
    .Select($"{TaskName}: Create security transaction", i => new SecurityTransaction
    {
        PortfolioId = i.Portfolio.Id,
        SecurityId = i.TargetSecurity.Id,
        OperationType = i.FileRow.OperationType=="RVP"? OperationType.Buy: OperationType.Sale,
        TransactionCode = $"{i.FileRow.PortfolioCode}-{i.FileRow.TradeDate:yyyyMMdd}-{i.FileRow.IsinCode}-{i.FileRow.GrossAmountInSecurityCcy.Value}",
        Description = i.FileRow.Description,
        TradeDate = i.FileRow.TradeDate,
        NavDate = i.FileRow.TradeDate,
        ValueDate = i.FileRow.ValueDate,
        Quantity = i.FileRow.Quantity.Value,
        FeesInSecurityCcy = i.FileRow.FeesInSecurityCcy,
        // //GrossAmountInPortfolioCcy = ...
        GrossAmountInSecurityCcy = i.FileRow.GrossAmountInSecurityCcy.Value,
        // //NetAmountInPortfolioCcy = ...
        NetAmountInSecurityCcy = i.FileRow.NetAmountInSecurityCcy,
        PriceInSecurityCcy = i.FileRow.PriceInSecurityCcy.Value,        
        BrokerId = i.Broker.Id, 
        TransactionType = TransactionType.SecurityMovement,
        DecisionType = TransactionDecisionType.Discretionary,
    }).EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedTransactionStream);

