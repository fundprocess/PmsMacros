var rbcCashFileDefinition = FlatFileDefinition.Create(i => new
{
    Brokerage = i.ToNumberColumn<double?>("BROKERAGE", "."),
    AccountCcy = i.ToColumn("ACCOUNT CCY"),
    SecurityCcy = i.ToColumn("SECURITY CCY"),
    IbanAccountNumber = i.ToColumn("IBAN ACCOUNT NUMBER"),
    ClosingDate = i.ToDateColumn("CLOSING DATE", "yyyyMMdd"),
    TransactionDescription = i.ToColumn("TRANSACTION DESCRIPTION"),
    Description = i.ToColumn("DESCRIPTION"),
    TransactionType = i.ToColumn("TRANSACTION TYPE"),
    SecurityName = i.ToColumn("SECURITY NAME"),
    ClientTransactionId = i.ToColumn("CLIENT TRANSACTION ID"),
    Fees = i.ToNumberColumn<double?>("FEES", "."),
    GrossAmount = i.ToNumberColumn<double?>("GROSS AMOUNT", "."),
    NetAmount = i.ToNumberColumn<double>("NET AMOUNT", "."),
    FundWebCode = i.ToColumn("FUND WEB CODE"),
    Price = i.ToNumberColumn<double?>("PRICE", "."),
    Quantity = i.ToNumberColumn<double?>("QUANTITY", "."),
    Reversal = i.ToBooleanColumn("REVERSAL", "Y", "N"), 
    Taxes = i.ToNumberColumn<double?>("TAXES", "."),
    TradeDate = i.ToOptionalDateColumn("TRADE DATE", "yyyyMMdd"),
    TransactionId = i.ToColumn("TRANSACTION ID"),
    IsinCode = i.ToColumn("ISIN CODE"),
    ValueDate = i.ToDateColumn("VALUE DATE", "yyyyMMdd"),
    FileName = i.ToSourceName()
}).IsColumnSeparated(';');

var savedCashMovementStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", rbcCashFileDefinition)
    .Select($"{TaskName}: extract IBAN and Fund Code", i => new
    {
        FileRow = i,
        Iban = i.IbanAccountNumber.Replace("IBAN ", "").Replace(" ", ""),
        FundCode = i.FundWebCode.Substring(0, 6),
        // LineCode = $"{i.FundWebCode}-{i.TransactionId}-{i.AccountCcy}-{i.Reversal}"
    })
    .EfCoreLookup($"{TaskName}: get related portfolio", o => o
        .Set<Portfolio>()
        .On(i => i.FundCode, i => i.InternalCode)
        .Select((l, r) => new { l.FileRow, l.Iban, Portfolio = r }))
    .Where($"{TaskName}: exclude cash movement unfound portfolio", i => i.Portfolio != null)
    .EfCoreLookup($"{TaskName}: get target account by iban", o => o
        .Set<Cash>()
        .On(i => i.Iban, i => i.Iban)
        .Select((l, r) => new { l.FileRow, l.Portfolio, TargetAccount = r }))
    // .Where($"{TaskName}: exclude movements with target account not found", i => i.TargetAccount != null)
    .EfCoreLookup($"{TaskName}: get underlying security by isin", o => o
        .Set<SecurityInstrument>()
        .On(i => string.Equals(i.FileRow.TransactionDescription, "cash transfer", StringComparison.InvariantCultureIgnoreCase) && string.Equals(i.FileRow.SecurityName, "DIVIDENDES D'ACTIONS", StringComparison.InvariantCultureIgnoreCase) ? i.FileRow.Description : i.FileRow.IsinCode, i => i.Isin)
        .Select((l, r) => new { l.FileRow, l.Portfolio, l.TargetAccount, UnderlyingSecurity = r }))
    .LookupCurrency($"{TaskName}: get currency", i => i.FileRow.SecurityCcy.ToLower(), (l, r) => new { l.FileRow, l.Portfolio, l.TargetAccount, l.UnderlyingSecurity, Currency = r })
    // .EntityFrameworkCoreLookup($"{TaskName}: get target security by internal code", dbStream, i => i.FromFile.CNId, (SecurityInstrument i) => i.InternalCode, (l, r) => new { l.FromFile, l.CurrencyId, l.Portfolio, TargetSecurity = l.TargetSecurity ?? r }, true)
    .Select($"{TaskName}: Create a sequence number based on the key", i => new { i.FileRow.FundWebCode, i.FileRow.TransactionId, i.FileRow.AccountCcy, i.FileRow.Reversal }, (i, seq) => new
    {
        i.FileRow,
        i.Currency,
        i.Portfolio,
        i.TargetAccount,
        i.UnderlyingSecurity,
        Sequence = seq
    })
    .Select($"{TaskName}: Create cash movement", i => CreateCashMovement(
        i.Portfolio.Id,
        i.TargetAccount?.Id,
        i.UnderlyingSecurity?.Id,
        i.FileRow.Brokerage,
        i.FileRow.IbanAccountNumber,
        i.FileRow.ClosingDate,
        i.FileRow.TransactionType,
        i.FileRow.SecurityName,
        i.FileRow.TransactionDescription,
        i.FileRow.ClientTransactionId,
        i.FileRow.Fees,
        i.FileRow.GrossAmount,
        i.FileRow.NetAmount,
        i.FileRow.FundWebCode,
        i.FileRow.Price,
        i.FileRow.Quantity,
        i.FileRow.Reversal,
        i.FileRow.Taxes,
        i.FileRow.TradeDate,
        i.FileRow.TransactionId,
        i.FileRow.IsinCode,
        i.FileRow.ValueDate,
        i.FileRow.AccountCcy,
        i.Sequence,
        i.Currency.Id
    ))
    .EfCoreSave($"{TaskName}: Save cash movement", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());
return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedCashMovementStream);

CashMovement CreateCashMovement(
    int portfolioId,
    int? targetAccountId,
    int? underlyingSecurityId,
    double? brokerage,
    string ibanAccountNumber,
    DateTime closingDate,
    string transactionType,
    string securityName,
    string transactionDescription,
    string clientTransactionId,
    double? fees,
    double? grossAmount,
    double netAmount,
    string fundWebCode,
    double? price,
    double? quantity,
    bool reversal,
    double? taxes,
    DateTime? tradeDate,
    string transactionId,
    string isin,
    DateTime valueDate,
    string accountCcy,
    int sequence,
    int currencyId
)
{
    (TransactionType transactionType, double sig) type = getTransactionType(transactionType, transactionDescription);
    var sig = reversal ? -type.sig : type.sig;
    return new CashMovement
    {
        CurrencyId = currencyId,
        BrokerageFeesInSecurityCcy = brokerage,
        CashId = targetAccountId,
        ClosingDate = closingDate,
        Description = securityName,
        ExternalTransactionCode = clientTransactionId,
        FeesInSecurityCcy = fees,
        GrossAmountInSecurityCcy = sig * (grossAmount ?? 0),
        MovementCode = transactionDescription,
        NetAmountInSecurityCcy = sig * netAmount,
        PortfolioId = portfolioId,
        PriceInSecurityCcy = price,
        Quantity = sig * quantity,
        Reversal = reversal,
        TaxesInSecurityCcy = taxes,
        TradeDate = tradeDate,
        TransactionCode = $"{fundWebCode}-{transactionId}-{accountCcy}-{sequence}{(reversal ? "-R" : (string)null)}",
        TransactionType = type.transactionType,
        UnderlyingSecurityId = underlyingSecurityId,
        ValueDate = valueDate
    };
}

(TransactionType transactionType, double sig) getTransactionType(string transactionType, string transactionDescription)
{
    transactionType = transactionType.ToUpper();
    transactionDescription = transactionDescription.ToUpper();
    if (transactionType == "P/S SECURITIES") return (TransactionType.SecurityMovement, 1);
    if (transactionType == "INCOME OUTGOING" && transactionDescription == "CASH ENTRY") return (TransactionType.Cash, 1);
    if (transactionType == "INCOME OUTGOING" && new[] { "PAYM. INVOICES", "CASH TRANSFER" }.Contains(transactionDescription)) return (TransactionType.Cash, -1);
    if (transactionType == "EXCHANGE" && transactionDescription == "SPOT EXCHANGE") return (TransactionType.SpotExchange, 1);
    if (transactionType == "INCOME/SECURIT." && transactionDescription == "DIVIDEND") return (TransactionType.Dividend, 1);
    if (transactionType == "SUBSCR./REDEMPT" && transactionDescription == "SUBSCRIPTION") return (TransactionType.SubscriptionRedemption, -1);
    if (transactionType == "SUBSCR./REDEMPT" && transactionDescription == "REDEMPTION") return (TransactionType.SubscriptionRedemption, 1);
    throw new Exception("unknown transaction type");
}
