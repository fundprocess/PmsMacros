string GetPortfolioCode()
    => "775400-UI";

//FILE MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    TransactionRefID = i.ToColumn("Transaction Ref ID"), // Transaction Ref ID	1.72203E+18
    ISIN = i.ToColumn("ISIN"), // ISIN	GB0031638363
    SecurityDesc = i.ToColumn("Security Desc"), // Security Desc	INTERTEK GROUP PLC /GBP/
    Type = i.ToColumn("Type"), // Type	RVP
    TransStatus = i.ToColumn("Trans Status"),// Trans Status	PD
    Units = i.ToNumberColumn<double?>("Units", "."), // Units	1467
    UnitPrice = i.ToNumberColumn<double?>("Unit Price", "."), // Unit Price	57.7054
    SECFee = i.ToNumberColumn<double?>("SEC Fee", "."), // SEC Fee	0
    Commission = i.ToNumberColumn<double?>("Commission ", "."), // Commission 	42.33
    CustodyFee = i.ToNumberColumn<double?>("Custody Fee", "."), // Custody Fee	0
    Principal = i.ToNumberColumn<double?>("Principal", "."), // Principal	84653.82
    Curr = i.ToColumn("Curr"), // Curr	GBP
    Location = i.ToColumn("Location"), // Location	GB
    MatchStatus = i.ToColumn("Match Status "), // Match Status 	MA
    //InstructionDate = i.ToDateColumn("Instruction Date","yyyy/MM/dd"), // Instruction Date	11/01/2021 11:36
    TradeDate = i.ToDateColumn("Trade Date","yyyy/MM/dd"), // Trade Date	2021/01/12
    SettleDate = i.ToDateColumn("Settle Date","yyyy/MM/dd"), // Settle Date	
    Amount = i.ToNumberColumn<double?>("Amount", "."), // Amount	85120.63
    ReasonCode = i.ToColumn("Reason Code"), // Reason Code	MACH
    ReasonCodeDesc = i.ToColumn("Reason Code Desc"), // Reason Code Desc	INSTRUCTION HAS BEEN MATCHED                                
    TradingBroker = i.ToColumn("Trading Broker"), // Trading Broker	BARCLAYS BANK IRELAND PLC
    ClearingBroker = i.ToColumn("Clearing Broker") // Clearing Broker	BARCLAYS BANK IRELAND PLC
}).IsColumnSeparated(',');

var secTransFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

#region broker and counterparties
var brokerCompanyStream = secTransFileStream
    .Where($"{TaskName} where tradingbroker has value", i=> !string.IsNullOrEmpty(i.TradingBroker))
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

#region Create Security Transaction
var subFundStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get securitiesStream", (ctx,j) => 
        ctx.Set<SubFund>().Where(i => i.InternalCode == GetPortfolioCode()))
        .EnsureSingle($"{TaskName}: ensures only one sub fund");

var savedTransactionStream = secTransFileStream
    .EfCoreLookup($"{TaskName}: get related target security", o => o.Set<SecurityInstrument>().On(i => i.ISIN, i=> i.Isin)
        .Select((l, r) => new { FileRow = l, TargetSecurity = r }).CacheFullDataset())
    .CorrelateToSingle($"{TaskName}: get broker by internal code", counterpartyStream, 
        (l, r) => new { FileRow = l.FileRow, TargetSecurity = l.TargetSecurity, Broker = r })
    .Select($"{TaskName}: Create security transaction",subFundStream, (i,j) => new SecurityTransaction
    {
        PortfolioId = j.Id,
        SecurityId = (i.TargetSecurity != null)? i.TargetSecurity.Id : throw new Exception("Security not found in database: "+i.FileRow.ISIN),
        OperationType = i.FileRow.Type == "RVP"? OperationType.Buy : (i.FileRow.Type == "DVP"? OperationType.Sale: throw new Exception("Unknown transaction type: " + i.FileRow.Type)),
        TransactionCode = "BBH-" + i.FileRow.TransactionRefID,
        Description = i.FileRow.SecurityDesc + " - " + i.FileRow.ReasonCodeDesc,
        TradeDate = i.FileRow.TradeDate,
        NavDate = i.FileRow.TradeDate,
        ValueDate = i.FileRow.TradeDate,
        SettlementDate = i.FileRow.SettleDate,
        Quantity = (i.FileRow.Units.HasValue)? i.FileRow.Units.Value 
                         : throw new Exception("Quantity not provided in Transaction: "+i.FileRow.TransactionRefID),
        FeesInSecurityCcy = i.FileRow.Commission,
        // //GrossAmountInPortfolioCcy = ...
        GrossAmountInSecurityCcy = (i.FileRow.Amount.HasValue)? i.FileRow.Amount.Value
                        : throw new Exception("Quantity not provided in Transaction: "+i.FileRow.TransactionRefID),
        // //NetAmountInPortfolioCcy = ...
        NetAmountInSecurityCcy = i.FileRow.Principal,
        PriceInSecurityCcy = (i.FileRow.UnitPrice.HasValue)? i.FileRow.UnitPrice.Value 
                          : throw new Exception("UnitPrice not provided: "+ i.FileRow.TransactionRefID),
        BrokerId = (i.Broker != null)? i.Broker.Id : throw new Exception("Broker not found: " + i.FileRow.TradingBroker),
        TransactionType = TransactionType.SecurityMovement,
        DecisionType = TransactionDecisionType.Discretionary,
    }).EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());
#endregion


return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedTransactionStream);

