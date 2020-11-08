var efaMovTransactionFileDefinition = FlatFileDefinition.Create(i => new
{
    AccntDate = i.ToDateColumn("Accnt_date", "dd/MM/yyyy"),
    BrokerCode = i.ToColumn("Broker_code"),
    BrokerName = i.ToColumn("Broker_name"),
    InOutPriceSubCcy = i.ToNumberColumn<double>("In_out_price_sub_ccy", "."),
    InOutPriceTrCcy = i.ToNumberColumn<double>("In_out_price_tr_ccy", "."),
    InputDate = i.ToDateColumn("Input_date", "dd/MM/yyyy"),
    InstrCode = i.ToColumn("Instr_code"),
    Icat = i.ToColumn("Icat"),
    Icy = i.ToColumn("Icy"),
    Mgroup = i.ToColumn("mgroup"),
    Movemcode = i.ToColumn("movemcode"),
    Mtyp = i.ToColumn("mtyp"), 
    Quantity = i.ToNumberColumn<double>("Quantity", "."),
    SFund = i.ToColumn("S_fund"),
    TransactionFees = i.ToNumberColumn<double>("Transaction_fees", "."),
    TransactionName = i.ToColumn("Transaction_name"),
    Transcode = i.ToColumn("transcode"),
    Ttyp = i.ToColumn("ttyp"),
    ValueDate = i.ToDateColumn("Value_date", "dd/MM/yyyy"),
    Isin = i.ToColumn("ISIN")
}).IsColumnSeparated(',');

var movFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", efaMovTransactionFileDefinition)
    .SetForCorrelation($"{TaskName}: prepare correlation");


var brokerStream = movFileStream
    .Distinct($"{TaskName}: exclude duplicate brokers", i => i.BrokerCode)
    .Where($"{TaskName}: exclude -1 brokers", i => i.BrokerCode != "-1")
    .Select($"{TaskName}: create brokers", i => new Company
    {
        InternalCode = i.BrokerCode,
        Name = i.BrokerName
    })
    .EfCoreSave($"{TaskName}: save brokers", o => o.SeekOn(i => i.InternalCode));

var movementsWithPortfolioStream = movFileStream
    .EfCoreLookup($"{TaskName}: get related portfolio", o => o
        .Set<Portfolio>()
        .On(i => i.SFund, i => i.InternalCode)
        .Select((l, r) => new { FileRow = l, Portfolio = r }))
    .Where($"{TaskName}: exclude movement unfound portfolio", i => i.Portfolio != null);

var savedSecurityTransactionStream = movementsWithPortfolioStream
    .Where($"{TaskName}: keep security rows only", i => !string.Equals(i.FileRow.Icat, "tres", StringComparison.InvariantCultureIgnoreCase) && !string.Equals(i.FileRow.Icat, "cpon", StringComparison.InvariantCultureIgnoreCase))
    .EfCoreLookup($"{TaskName}: get target security by internal code", o => o
        .Set<SecurityInstrument>()
        .On(i => i.FileRow.InstrCode, i => i.InternalCode)
        .Select((l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r }))
    .Where($"{TaskName}: exclude movements with target security not found", i => i.TargetSecurity != null)
    .CorrelateToSingle($"{TaskName}: get broker by internal code", brokerStream, (l, r) => new { l.FileRow, l.Portfolio, l.TargetSecurity, Broker = r })
    .Select($"{TaskName}: Create security transaction", i => CreateSecurityTransaction(
        i.Portfolio.Id,
        i.TargetSecurity.Id,
        i.Broker.Id,
        i.FileRow.AccntDate,
        i.FileRow.BrokerCode,
        i.FileRow.BrokerName,
        i.FileRow.InOutPriceSubCcy,
        i.FileRow.InOutPriceTrCcy,
        i.FileRow.InputDate,
        i.FileRow.InstrCode,
        i.FileRow.Isin,
        i.FileRow.Icat,
        i.FileRow.Mgroup,
        i.FileRow.Movemcode,
        i.FileRow.Mtyp,
        i.FileRow.Quantity,
        i.FileRow.SFund,
        i.FileRow.TransactionFees,
        i.FileRow.TransactionName,
        i.FileRow.Transcode,
        i.FileRow.Ttyp,
        i.FileRow.ValueDate
    ))
    .EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode));

var savedCashMovementStream = movementsWithPortfolioStream
    .Where($"{TaskName}: keep cash rows only", i => string.Equals(i.FileRow.Icat, "tres", StringComparison.InvariantCultureIgnoreCase) || string.Equals(i.FileRow.Icat, "cpon", StringComparison.InvariantCultureIgnoreCase))
    .EfCoreLookup($"{TaskName}: get underlying security by isin", o => o
        .Set<SecurityInstrument>()
        .On(i => i.FileRow.Isin, i => i.Isin)
        .Select((l, r) => new { l.FileRow, l.Portfolio, UnderlyingSecurity = r }))
    .LookupCurrency($"{TaskName}: get currency", i => i.FileRow.Icy, (l, r) => new { l.FileRow, l.Portfolio, l.UnderlyingSecurity, Currency = r })
    .Select($"{TaskName}: Create cash movement", i => CreateCashMovement(
        i.Portfolio.Id,
        null,
        i.FileRow.AccntDate,
        i.FileRow.BrokerCode,
        i.FileRow.BrokerName,
        i.FileRow.InOutPriceSubCcy,
        i.FileRow.InOutPriceTrCcy,
        i.FileRow.InputDate,
        i.FileRow.InstrCode,
        i.FileRow.Isin,
        i.FileRow.Icat,
        i.FileRow.Mgroup,
        i.FileRow.Movemcode,
        i.FileRow.Mtyp,
        i.FileRow.Quantity,
        i.FileRow.SFund,
        i.FileRow.TransactionFees,
        i.FileRow.TransactionName,
        i.FileRow.Transcode,
        i.FileRow.Ttyp,
        i.FileRow.ValueDate,
        i.UnderlyingSecurity?.Id,
        i.Currency.Id
    ))
    .EfCoreSave($"{TaskName}: Save cash movement", o => o.SeekOn(i => i.TransactionCode));
return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedSecurityTransactionStream, savedCashMovementStream);


CashMovement CreateCashMovement(
    int portfolioId,
    int? targetAccountId,
    DateTime accntDate,
    string brokerCode,
    string brokerName,
    double inOutPriceSubCcy,
    double inOutPriceTrCcy,
    DateTime inputDate,
    string instrCode,
    string iSIN,
    string lcat,
    string mgroup,
    string movemcode,
    string mtyp,
    double quantity,
    string sFund,
    double transactionFees,
    string transactionName,
    string transcode,
    string ttyp,
    DateTime valueDate,
    int? underlyingSecurityId,
    int currencyId
)
{
    TransactionType transactionType = MapTransactionType(lcat, ttyp);
    OperationType operationType = MapOperationType(mgroup, mtyp);
    double sig = operationType == OperationType.Purchase ? -1 : 1;
    var netAmountInPortfolioCcy = inOutPriceTrCcy != 0
        ? sig * (inOutPriceTrCcy - transactionFees) * (inOutPriceSubCcy / inOutPriceTrCcy)
        : 0;

    return new CashMovement
    {
        CashId = targetAccountId,
        ClosingDate = accntDate,
        Description = transactionName,
        ExternalTransactionCode = ttyp,
        FeesInSecurityCcy = transactionFees,
        GrossAmountInPortfolioCcy = sig * inOutPriceSubCcy,
        GrossAmountInSecurityCcy = sig * inOutPriceTrCcy,
        MovementCode = movemcode,
        NetAmountInPortfolioCcy = netAmountInPortfolioCcy,
        NetAmountInSecurityCcy = sig * (inOutPriceTrCcy - transactionFees),
        PortfolioId = portfolioId,
        PriceInSecurityCcy = (inOutPriceTrCcy - transactionFees) / quantity,
        Quantity = sig * quantity,
        Reversal = false,
        TradeDate = inputDate,
        TransactionCode = $"{transcode}-{movemcode}",
        TransactionType = transactionType,
        UnderlyingSecurityId = underlyingSecurityId,
        CurrencyId = currencyId
    };
}

SecurityTransaction CreateSecurityTransaction(
    int portfolioId,
    int targetSecurityId,
    int brokerId,
    DateTime accntDate,
    string brokerCode,
    string brokerName,
    double inOutPriceSubCcy,
    double inOutPriceTrCcy,
    DateTime inputDate,
    string instrCode,
    string iSIN,
    string lcat,
    string mgroup,
    string movemcode,
    string mtyp,
    double quantity,
    string sFund,
    double transactionFees,
    string transactionName,
    string transcode,
    string ttyp,
    DateTime valueDate
)
{
    TransactionType transactionType = MapTransactionType(lcat, ttyp);
    OperationType operationType = MapOperationType(mgroup, mtyp);
    double sig = operationType == OperationType.Purchase ? -1 : 1;
    var netAmountInPortfolioCcy = inOutPriceTrCcy != 0
        ? sig * (inOutPriceTrCcy - transactionFees) * (inOutPriceSubCcy / inOutPriceTrCcy)
        : 0;
    return new SecurityTransaction
    {
        Description = transactionName,
        NavDate = accntDate,
        PortfolioId = portfolioId,
        TradeDate = inputDate,
        TransactionCode = $"{transcode}-{movemcode}",
        FeesInSecurityCcy = transactionFees,
        GrossAmountInPortfolioCcy = sig * inOutPriceSubCcy,
        GrossAmountInSecurityCcy = sig * inOutPriceTrCcy,
        NetAmountInPortfolioCcy = netAmountInPortfolioCcy,
        NetAmountInSecurityCcy = sig * (inOutPriceTrCcy - transactionFees),
        OperationType = operationType,
        PriceInSecurityCcy = inOutPriceTrCcy / quantity,
        Quantity = sig * quantity,
        SecurityId = targetSecurityId,
        TransactionType = transactionType,
        ValueDate = valueDate

    };
}

OperationType MapOperationType(string mgroup, string mtyp)
{
    mgroup = (mgroup ?? mtyp.Substring(0, 2)).ToLower();
    switch (mgroup)
    {
        case "e": // entree
        case "et": // recette
        case "rt": // recette
            return OperationType.Sale;
        case "s": // sortie
        case "st": // sortie
        case "dt": // depense
            return OperationType.Purchase;
    }
    throw new Exception("Unknown operation type");
}

TransactionType MapTransactionType(string lcat, string ttyp)
{
    lcat = lcat.ToUpper();
    ttyp = ttyp.ToUpper();

    switch (lcat)
    {
        case "TRES":
            if (ttyp == "ITR")
                return TransactionType.Interest;
            else
                return TransactionType.Cash;
        case "VMOB":
        case "FUTU":
        case "OPTI":
            return TransactionType.SecurityMovement;
        case "CPON":
            if (ttyp == "DTDV")
                return TransactionType.Dividend;
            else
                return TransactionType.Coupon;
        case "CAT":
            return TransactionType.SpotExchange;
    }
    switch (ttyp)
    {
        case "FGS": return TransactionType.ManagementFees;
        case "SOQ": return TransactionType.SubscriptionRedemption;
        case "RCH": return TransactionType.SubscriptionRedemption;
    }
    throw new Exception("Unknown transaction type");
}
