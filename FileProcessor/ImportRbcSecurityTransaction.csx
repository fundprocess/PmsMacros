var rbcTransactionFileDefinition = FlatFileDefinition.Create(i => new
{
    DealDescription = i.ToColumn("Deal Description"),
    NavDate = i.ToDateColumn("Nav Date", "yyyyMMdd"),
    FundCode = i.ToColumn("Fund Code"),
    TradeDate = i.ToDateColumn("Trade Date", "yyyyMMdd"),
    CNId = i.ToColumn("CNId"),
    NEntryPtf = i.ToColumn("N Entry Ptf"),
    NEntry = i.ToColumn("N Entry"),
    FeesLocalCcy = i.ToNumberColumn<double?>("Fees Local Ccy", "."),
    PurchAmountLocalCcyGross = i.ToNumberColumn<double>("Purch Amount local Ccy Gross", "."),
    PurchAmountFundCcyNet = i.ToNumberColumn<double>("Purch Amount Fund Currency net", "."),
    PurchAmountLocalCcyNet = i.ToNumberColumn<double>("Purch Amount Local Ccy Net", "."),
    SalesAmountLocalCcyGross = i.ToNumberColumn<double>("Sales Amount local Currency gross", "."),
    SalesAmountFundCcyNet = i.ToNumberColumn<double>("Sales Amount Fund Ccy net", "."),
    SalesAmountLocalCcyNet = i.ToNumberColumn<double>("Sales Amount Local Ccy net", "."),
    VXchgeRate = i.ToNumberColumn<double>("v_Xchge_Rate", "."),
    PriceLocalCcy = i.ToNumberColumn<double>("Price Local Ccy", "."),
    Quantity = i.ToNumberColumn<double>("Quantity", "."),
    IsinCode = i.ToColumn("ISIN Code"),
    OperationType = i.ToColumn("Operation Type"),
    ValueDate = i.ToDateColumn("Value Date", "yyyyMMdd"),
    FileName = i.ToSourceName()
}).IsColumnSeparated(',');

var transFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", rbcTransactionFileDefinition);

var savedTransactionStream = transFileStream
    .EfCoreLookup($"{TaskName}: get related portfolio", o => o
        .Set<Portfolio>()
        .On(i => i.FundCode, i => i.InternalCode)
        .Select((l, r) => new { FileRow = l, Portfolio = r })
        .CacheFullDataset())
    .Where($"{TaskName}: exclude transaction with unfound portfolio", i => i.Portfolio != null)
    .EfCoreLookup($"{TaskName}: get target security by isin", o => o
        .Set<SecurityInstrument>()
        .On(i => i.FileRow.IsinCode, i => i.Isin)
        .Select((l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r })
        .CacheFullDataset())
    .Where($"{TaskName}: exclude movements with target security not found", i => i.TargetSecurity != null) // TODO: check why not every security matches
    .Select($"{TaskName}: Create security transaction", i => CreateSecurityTransaction(
        i.Portfolio.Id,
        i.TargetSecurity.Id,
        i.FileRow.DealDescription,
        i.FileRow.NavDate,
        i.FileRow.FundCode,
        i.FileRow.TradeDate,
        i.FileRow.CNId,
        i.FileRow.NEntryPtf,
        i.FileRow.NEntry,
        i.FileRow.FeesLocalCcy,
        i.FileRow.PurchAmountLocalCcyGross,
        i.FileRow.PurchAmountFundCcyNet,
        i.FileRow.PurchAmountLocalCcyNet,
        i.FileRow.SalesAmountLocalCcyGross,
        i.FileRow.SalesAmountFundCcyNet,
        i.FileRow.SalesAmountLocalCcyNet,
        i.FileRow.VXchgeRate,
        i.FileRow.PriceLocalCcy,
        i.FileRow.Quantity,
        i.FileRow.IsinCode,
        i.FileRow.OperationType,
        i.FileRow.ValueDate
    ))
    .EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode));
return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedTransactionStream);

SecurityTransaction CreateSecurityTransaction(
    int portfolioId,
    int securityId,
    string dealDescription,
    DateTime navDate,
    string fundCode,
    DateTime tradeDate,
    string cNId,
    string nEntryPtf,
    string nEntry,
    double? feesLocalCcy,
    double purchAmountLocalCcyGross,
    double purchAmountFundCcyNet,
    double purchAmountLocalCcyNet,
    double salesAmountLocalCcyGross,
    double salesAmountFundCcyNet,
    double salesAmountLocalCcyNet,
    double vXchgeRate,
    double priceLocalCcy,
    double quantity,
    string isinCode,
    string operationType,
    DateTime valueDate
)
{
    var type = string.Equals(operationType, "purch", StringComparison.InvariantCultureIgnoreCase) ? OperationType.Purchase : OperationType.Sale;
    return new SecurityTransaction
    {
        PortfolioId = portfolioId,
        SecurityId = securityId,
        OperationType = type,
        // TransactionCode = cNId,
        TransactionType = TransactionType.SecurityMovement,
        Description = dealDescription,
        TradeDate = tradeDate,
        ValueDate = valueDate,
        NavDate = navDate,
        Quantity = quantity,
        PriceInSecurityCcy = priceLocalCcy,

        GrossAmountInSecurityCcy = purchAmountLocalCcyGross + salesAmountLocalCcyGross,
        NetAmountInSecurityCcy = purchAmountLocalCcyNet + salesAmountLocalCcyNet,
        NetAmountInPortfolioCcy = purchAmountFundCcyNet + salesAmountFundCcyNet,
        FeesInSecurityCcy = feesLocalCcy,
        GrossAmountInPortfolioCcy = (purchAmountLocalCcyGross + salesAmountLocalCcyGross) / vXchgeRate,

        TransactionCode = $"{tradeDate:yyyyMMdd}-{cNId}-{nEntryPtf}-{nEntry}"
    };
}
