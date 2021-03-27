//FILE MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    BalanceDate = i.ToOptionalDateColumn("Balance Date " , "MM/dd/yyyy"),
    CurrencyCode = i.ToColumn("Currency Code "),
    MainAccount = i.ToColumn("Main Account"),
    // BalanceFloat1Day = i.ToNumberColumn<double?>("Balance Float  - 1 Day",","),
    // BalanceFloat2Day = i.ToNumberColumn<double?>("Balance Float  - 2 Day",","),
    // BalanceFloat3Day = i.ToNumberColumn<double?>("Balance Float  - 3 Day,",""),
    BalanceType = i.ToNumberColumn<int?>("Balance Type","."),
    // ClosingAvailable+CMSSweepOut = i.ToColumn("Closing Available + CMS Sweep Out"),
    // ClosingAvailableBalance = i.ToColumn("Closing Available Balance"),
    // ClosingLedger+CMSSweepOut = i.ToColumn("Closing Ledger + CMS Sweep Out"),
    ClosingLedgerBalance = i.ToNumberColumn<double?>("Closing Ledger Balance ",".",","),
    CurrencyAccount = i.ToColumn("Currency Account"),
    CurrencyAccountName = i.ToColumn("Currency Account Name"),
    // OpeningAvailable+CMSSweepReturn = i.ToNumberColumn<double?>("Opening Available + CMS Sweep Return",","),
    // OpeningAvailableBalance = i.ToNumberColumn<double?>("Opening Available Balance",","),
    // OpeningLedger+CMSSweepReturn = i.ToNumberColumn<double?>("Opening Ledger + CMS Sweep Return",","),
    OpeningLedgerBalance = i.ToNumberColumn<double?>("Opening Ledger Balance",".",","),
    TotalCredits = i.ToNumberColumn<double?>("Total Credits",".",","),
    TotalDebits = i.ToNumberColumn<double?>("Total Debits",".",","),
    ValueDate = i.ToOptionalDateColumn("Value Date","MM/dd/yyyy"),
    ActionType = i.ToColumn("Action Type"),
    //CAID = i.ToColumn("CA ID"),
    CurrentPaydownFactor = i.ToNumberColumn<double?>("Current Paydown Factor",".",","),
    EXEligibilityDate = i.ToOptionalDateColumn("EX/Eligibility Date" , "MM/dd/yyyy"),
    FATCAWitholdingAmount = i.ToNumberColumn<double?>("FATCA Witholding Amount",".",","),
    FATCAWitholdingTaxRate = i.ToNumberColumn<double?>("FATCA Witholding Tax Rate",".",","),
    GrossDividendInterestRate = i.ToNumberColumn<double?>("Gross Dividend/Interest Rate ",".",","),
    //PayDate = i.ToOptionalDateColumn("Pay Date" , "MM/dd/yyyy"),
    PriorPaydownFactor = i.ToNumberColumn<double?>("Prior Paydown Factor",".",","),
    ReclaimAmount = i.ToNumberColumn<double?>("Reclaim Amount",".",","),
    ReclaimRate = i.ToNumberColumn<double?>("Reclaim Rate",".",","),
    RecordDate = i.ToOptionalDateColumn("Record Date" , "MM/dd/yyyy"),
    TaxWithheld1 = i.ToColumn("Tax Withheld-1"),
    TaxWithheld2 = i.ToColumn("Tax Withheld-2"),
    WithholdingRate = i.ToNumberColumn<double?>("Withholding Rate",".",","),
    CheckNumber = i.ToNumberColumn<double?>("Check Number",".",","),
    FXContraCurrencyAmount = i.ToNumberColumn<double?>("FX Contra Currency Amount  ",".",","),
    FXContraCurrencyCode = i.ToColumn("FX Contra Currency Code "),
    FXDealRate = i.ToNumberColumn<double?>("FX Deal Rate",".",","),
    FedReferenceNumber = i.ToColumn("Fed Reference Number"),
    OriginatingBankBIC = i.ToColumn("Originating Bank BIC "),
    PaymentID = i.ToColumn("Payment ID"),
    ActualSettlementDate = i.ToOptionalDateColumn("Actual Settlement Date" , "MM/dd/yyyy"),
    AdjustedBalance = i.ToColumn("Adjusted Balance"),
    BBHSONICReferenceNum = i.ToColumn("BBH/SONIC Reference Num"),
    BankofDeposit = i.ToColumn("Bank of Deposit"),
    CASTransactionCode = i.ToColumn("CAS Transaction Code"),
    ClearingBrokerFINID = i.ToColumn("Clearing Broker FIN ID"),
    ClientReferenceNum = i.ToColumn("Client Reference Num"),
    CommissionAmount = i.ToNumberColumn<double?>("Commission amount",".",","),
    ContractualSettlementDate = i.ToOptionalDateColumn("Contractual Settlement Date" , "MM/dd/yyyy"),
    CreationDate = i.ToOptionalDateColumn("Creation Date" , "MM/dd/yyyy"),
    DaysinOverdraft = i.ToColumn("Days in Overdraft"),
    DebitCreditIndicator = i.ToColumn("Debit/Credit Indicator"),
    FeeAmount = i.ToNumberColumn<double?>("Fee  Amount",".",","),
    GrossAmount = i.ToNumberColumn<double?>("Gross Amount",".",","),
    NetSettlementAmount = i.ToNumberColumn<double?>("Net Settlement Amount ",".",","),
    NextDayFunds = i.ToNumberColumn<double?>("Next Day Funds",".",","),
    OverdraftInterestAccrued = i.ToNumberColumn<double?>("Overdraft Interest Accrued",".",","),
    OverdraftRate = i.ToNumberColumn<double?>("Overdraft Rate",".",","),
    PostingDate = i.ToOptionalDateColumn("Posting Date" , "MM/dd/yyyy"),
    PostingDateNAV = i.ToNumberColumn<double?>("Posting Date NAV",".",","),
    //SameDayFunds = i.ToNumberColumn<double?>("Same Day Funds",","),
    // SettledTimestamp = i.ToColumn("Settled Timestamp"),
    // SettlementLocation = i.ToColumn("Settlement Location"),
    // StockLoanDescription = i.ToColumn("Stock Loan Description"),
    // StockLoanIndicator = i.ToColumn("Stock Loan Indicator"),
    SystemReferenceNumber = i.ToColumn("System Reference Number"),
    //ThreeDayFunds = i.ToColumn("Three Day Funds"),
    TradeDate = i.ToOptionalDateColumn("Trade Date" , "MM/dd/yyyy"),
    TradingBrokerFINID = i.ToColumn("Trading Broker FIN ID"),
    TradingBrokerName = i.ToColumn("Trading Broker Name"),
    TransactionAmount = i.ToNumberColumn<double?>("Transaction Amount",".",","),
    TransactionCategory = i.ToColumn("Transaction Category"),
    TransactionDescription = i.ToColumn("Transaction Description"),
    TransactionGroup = i.ToColumn("Transaction Group"),
    TwoDayFunds = i.ToNumberColumn<double?>("Two Day Funds",".",","),
    UnitPrice = i.ToNumberColumn<double?>("Unit Price",".",","),
    Units = i.ToNumberColumn<double?>("Units",".",","),
    BBHSecurityNumber = i.ToColumn("BBH Security Number"),
    CINS = i.ToColumn("CINS"),
    CUSIP = i.ToColumn("CUSIP"),
    ISIN = i.ToColumn("ISIN"),
    MaturityDate = i.ToOptionalDateColumn("Maturity Date" , "MM/dd/yyyy"),
    SecurityID = i.ToColumn("Security ID"),
    SecurityShortDescription = i.ToColumn("Security Short Description"),
    Sedol = i.ToColumn("Sedol"),
    Sicovam = i.ToColumn("Sicovam"),
    Ticker = i.ToColumn("Ticker"),
    Valoren = i.ToColumn("Valoren"),
}).IsColumnSeparated(',');

var fileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key")
    .Where("", i=> ! string.IsNullOrWhiteSpace(i.SystemReferenceNumber));
var cashMovFileStream = fileStream 
    .Where($"{TaskName} Only Cash Mov lines except fx",i => !string.IsNullOrEmpty(i.TransactionCategory) && i.TransactionCategory != "Foreign Exchange");
var fxTransactionsFileStream = fileStream 
    .Where($"{TaskName} Only Cash Fx Mov lines",i => !string.IsNullOrEmpty(i.TransactionCategory) && i.TransactionCategory == "Foreign Exchange");

#region broker and counterparties
var brokerCompanyStream = fileStream
    .Distinct($"{TaskName}: exclude duplicate brokers", i => i.TradingBrokerName)
    .Select($"{TaskName}: create broker companies", i => new Company
    {
        InternalCode = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.TradingBrokerName.Replace(" ", "").Trim()),
        Name = i.TradingBrokerName
    }).EfCoreSave($"{TaskName}: save broker companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var bankOfDespositCompanyStream = fileStream
    .Where($"{TaskName} Filter empty BankofDeposit",i=> !string.IsNullOrWhiteSpace(i.BankofDeposit))
    .Distinct($"{TaskName}: exclude duplicate BankofDeposit", i => i.BankofDeposit)
    .Select($"{TaskName}: create BankofDeposit companies", i => new Company
    {
        InternalCode = i.BankofDeposit.ToLower()=="bbh"? "bbh" : CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.BankofDeposit.Replace(" ", "").Trim()),
        Name = i.BankofDeposit,
    }).EfCoreSave($"{TaskName}: save BankofDeposit companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var counterpartyStream = brokerCompanyStream.Union($"{TaskName} Union of brokers-Depositary",bankOfDespositCompanyStream)
    .Distinct($"{TaskName}: exclude duplicate counterparty", i => i.InternalCode)
    .Select($"{TaskName}: create counterparty", euroCurrency, (i, j) => new CounterpartyRelationship
    {
        EntityId = i.Id,
        StartDate = DateTime.Today,
        CounterpartyType = CounterpartyType.Broker,
        CurrencyId = j.Id,
        EmirClassification = EmirClassification.Financial,
    }).EfCoreSave($"{TaskName}: save counterparty", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());
#endregion broker and counterparties

var portfoliosStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get portfolios", (ctx,j) => ctx.Set<Portfolio>());
var securitiesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get securities", (ctx,j) => ctx.Set<SecurityInstrument>());
var securityTransactionsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get security transactions", 
    (ctx,j) => ctx.Set<SecurityTransaction>().Include(i => i.Security).Where(i=>!i.TransactionCode.Contains("BBH - SubRed -")));

var savedCashMovementsStream = cashMovFileStream
    .Lookup($"{TaskName} Lookup related portfolio by name",portfoliosStream, 
        i => i.CurrencyAccountName.Split(" - ")[1].Trim().ToLower(), 
        i => i.Name.ToLower(),
        (l,r) => new {FileRow = l, Portfolio = r})
    .LookupCurrency($"{TaskName}: get currency", i => i.FileRow.CurrencyCode, 
        (l, r) => new { l.FileRow, l.Portfolio, Currency = r})
    .CorrelateToSingle($"{TaskName}: get broker by internal code", counterpartyStream, 
        (l, r) => new { l.FileRow, l.Portfolio, l.Currency, Broker = r})
    .Lookup($"{TaskName} Lookup related security",securitiesStream, i => i.FileRow.ISIN, i => i.InternalCode,
        (l, r) => new { l.FileRow, l.Portfolio, l.Currency, l.Broker, Security = r })
    .Lookup($"{TaskName} Lookup related sec trans",securityTransactionsStream, 
        i => i.FileRow.SystemReferenceNumber, //1722027040000
        i => i.TransactionCode.Replace("BBH-","").Substring(0,13), //BBH-1722027040000000000
        // i => i.FileRow.ISIN + ((i.FileRow.TradeDate.HasValue)? i.FileRow.TradeDate.Value.ToString("yyyyMMdd") : ((i.FileRow.ValueDate.HasValue)? i.FileRow.ValueDate.Value.ToString("yyyyMMdd") : i.FileRow.BalanceDate.Value.ToString("yyyyMMdd"))),
        // i => i.Security.Isin+i.TradeDate.ToString("yyyyMMdd"),
        (l, r) => new { l.FileRow, l.Portfolio, l.Currency, l.Broker, l.Security, SecTrans = r })

    .Select($"{TaskName}: Create cash movement", i => new CashMovement{
        PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found for: " + i.FileRow.CurrencyAccountName.Split(" - ")[1].Trim()),
        CurrencyId = (i.Currency != null)? i.Currency.Id : throw new Exception("Currency not found for: " + i.FileRow.CurrencyAccountName),

        MovementCode = "BBH-"+i.FileRow.SystemReferenceNumber,
        TransactionCode = i.FileRow.TransactionCategory=="Security Activity"? i.FileRow.ISIN + i.FileRow.TradeDate.Value.ToString("yyyyMMdd") 
                            : i.FileRow.SystemReferenceNumber,

        TransactionType = (i.FileRow.TransactionCategory == "SecurityActivity")? TransactionType.SecurityMovement:
                            (i.FileRow.TransactionCategory == "ShortTermInvestment")? TransactionType.Cash:
                            TransactionType.Cash,

        ValueDate = (i.FileRow.ValueDate != null)? i.FileRow.ValueDate.Value : i.FileRow.BalanceDate.Value,
        ClosingDate = (i.FileRow.ActualSettlementDate != null)? i.FileRow.ActualSettlementDate.Value : ((i.FileRow.ValueDate != null)? i.FileRow.ValueDate.Value : i.FileRow.BalanceDate.Value),
        TradeDate =  (i.FileRow.TradeDate != null)? i.FileRow.TradeDate.Value : ((i.FileRow.CreationDate != null)? i.FileRow.CreationDate.Value : (DateTime?) null),

        Description = i.FileRow.TransactionGroup + "-" + i.FileRow.TransactionCategory + "-" +i.FileRow.TransactionDescription,
        ExternalTransactionCode = i.FileRow.ClientReferenceNum,

        GrossAmountInSecurityCcy = (i.FileRow.TransactionAmount.HasValue)? i.FileRow.TransactionAmount.Value: (double?) null,
        // double? GrossAmountInPortfolioCcy
        NetAmountInSecurityCcy = (i.FileRow.NetSettlementAmount.HasValue)? i.FileRow.NetSettlementAmount.Value: (double?) null, 
        //NetAmountInPortfolioCcy, 
        PriceInSecurityCcy =  (i.FileRow.UnitPrice.HasValue)? i.FileRow.UnitPrice.Value: (double?) null,
        Quantity = i.FileRow.Units.Value,
        Reversal = false,
        TaxesInSecurityCcy = (i.FileRow.CommissionAmount.HasValue)? i.FileRow.CommissionAmount.Value: (double?) null,
        CounterpartyId = i.Broker.Id,
        FeesInSecurityCcy = (i.FileRow.FeeAmount.HasValue)? i.FileRow.FeeAmount.Value : (double?) null,
        BrokerageFeesInSecurityCcy = (i.FileRow.CommissionAmount.HasValue)? i.FileRow.CommissionAmount.Value : (double?) null,
        // int? CashId
        UnderlyingSecurityId = (i.Security != null)? i.Security.Id: (int?) null,
        TransactionId = (i.SecTrans != null)? i.SecTrans.Id: (int?) null,
    })
    .EfCoreSave($"{TaskName}: Save cash movement", o => o.SeekOn(i => i.MovementCode).DoNotUpdateIfExists());

var savedFxCashMovementsStream = fxTransactionsFileStream
    .Lookup($"{TaskName} FxMov> Lookup related portfolio by name",portfoliosStream, 
        i => i.CurrencyAccountName.Split(" - ")[1].Trim().ToLower(), i => i.Name.ToLower(),
        (l,r) => new {FileRow = l, Portfolio = r})
    .LookupCurrency($"{TaskName}: FxMov> get currencyFrom", i => i.FileRow.CurrencyCode, 
        (l, r) => new { l.FileRow, l.Portfolio, CurrencyFrom = r})
    .LookupCurrency($"{TaskName}: FxMov> get currencyTo", i => i.FileRow.FXContraCurrencyCode, 
        (l, r) => new { l.FileRow, l.Portfolio, l.CurrencyFrom , CurrencyTo = r})
    .CorrelateToSingle($"{TaskName}: FxMov> get broker by internal code", bankOfDespositCompanyStream, 
        (l, r) => new { l.FileRow, l.Portfolio, l.CurrencyFrom, l.CurrencyTo, Depositary = r})
    .Lookup($"{TaskName} FxMov> Lookup related security",securitiesStream, i => i.FileRow.ISIN, i => i.InternalCode,
        (l, r) => new { l.FileRow, l.Portfolio, l.CurrencyFrom, l.CurrencyTo, l.Depositary, Security = r })
    .Select($"{TaskName}: FxMov> Create cash movement", i => new CashMovement{
        PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found for: " + i.FileRow.CurrencyAccountName.Split(" - ")[1].Trim()),
        CurrencyId = (i.CurrencyTo != null)? i.CurrencyTo.Id : throw new Exception("Currency to not found for: " + i.FileRow.CurrencyAccountName),

        MovementCode = "BBH-"+i.FileRow.SystemReferenceNumber,
        TransactionCode = i.FileRow.TransactionCategory=="Security Activity"? i.FileRow.ISIN + i.FileRow.TradeDate.Value.ToString("yyyyMMdd") 
                            : i.FileRow.SystemReferenceNumber,

        TransactionType = TransactionType.SpotExchange,

        ValueDate = (i.FileRow.ValueDate != null)? i.FileRow.ValueDate.Value 
                    : ((i.FileRow.BalanceDate != null)? i.FileRow.BalanceDate.Value: throw new Exception("no date for "+i.FileRow.SystemReferenceNumber)),
        ClosingDate = (i.FileRow.ActualSettlementDate != null)? i.FileRow.ActualSettlementDate.Value 
                : ((i.FileRow.ValueDate != null)? i.FileRow.ValueDate.Value : 
                ((i.FileRow.BalanceDate != null)? i.FileRow.BalanceDate.Value: throw new Exception("no date for "+i.FileRow.SystemReferenceNumber))),
        TradeDate =  (i.FileRow.TradeDate != null)? i.FileRow.TradeDate.Value : ((i.FileRow.CreationDate != null)? 
                        i.FileRow.CreationDate.Value : (DateTime?) null),

        Description = i.FileRow.TransactionGroup + "-" +i.FileRow.TransactionDescription,
        ExternalTransactionCode = i.FileRow.ClientReferenceNum,
        
        GrossAmountInSecurityCcy = (i.FileRow.FXContraCurrencyAmount.HasValue)? i.FileRow.FXContraCurrencyAmount.Value: (double?) null,
        GrossAmountInPortfolioCcy = (i.FileRow.TransactionAmount.HasValue)? i.FileRow.TransactionAmount.Value: (double?) null, 
        NetAmountInSecurityCcy = (i.FileRow.TransactionAmount.HasValue)? i.FileRow.TransactionAmount.Value: (double?) null,
        NetAmountInPortfolioCcy = (i.FileRow.NetSettlementAmount.HasValue)? i.FileRow.NetSettlementAmount.Value: (double?) null, 
        TaxesInSecurityCcy = (i.FileRow.CommissionAmount.HasValue)? i.FileRow.CommissionAmount.Value: (double?) null,

        //PriceInSecurityCcy = i.FileRow.UnitPrice.Value,
        //Quantity = i.FileRow.Units.Value,
        Reversal = false,
        CounterpartyId = i.Depositary != null? i.Depositary.Id : throw new Exception("Depositary not found: "+i.FileRow.BankofDeposit),
        FeesInSecurityCcy = (i.FileRow.FeeAmount.HasValue)? i.FileRow.FeeAmount.Value : (double?) null,
        BrokerageFeesInSecurityCcy = (i.FileRow.CommissionAmount.HasValue)? i.FileRow.CommissionAmount.Value : (double?) null,
        // int? CashId
        UnderlyingSecurityId = (i.Security != null)? i.Security.Id: (int?) null,
    })
    .EfCoreSave($"{TaskName}: FxMov> Save cash movement", o => o.SeekOn(i => i.MovementCode).DoNotUpdateIfExists());


return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedCashMovementsStream) ;