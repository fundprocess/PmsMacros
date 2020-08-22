var rbcFxFileDefinition = FlatFileDefinition.Create(i => new
{
    BrokerCode = i.ToColumn("Broker Code"),
    BrokerName = i.ToColumn("Broker Name"),
    Description = i.ToColumn("Description"),
    NavDate = i.ToDateColumn("NAV Date", "yyyyMMdd"),
    Fund = i.ToColumn("Fund"),
    TradeDate = i.ToDateColumn("Trade Date", "yyyyMMdd"),
    ExchangeRate = i.ToNumberColumn<double>("Exchange Rate", "."),
    PurchaseCcy = i.ToColumn("Purchase CCY"),
    PurchasedAmount = i.ToNumberColumn<double>("Purchased Amount", "."),
    SettlementDate = i.ToDateColumn("Settlement Date", "yyyyMMdd"),
    SoldAmount = i.ToNumberColumn<double>("Sold Amount", "."),
    SoldCcy = i.ToColumn("Sold CCY"),
    ContractNo = i.ToColumn("Contract No"),
}).IsColumnSeparated(',');

var transFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", rbcFxFileDefinition)
    .SetForCorrelation($"{TaskName}: prepare correlation");


var brokerStream = transFileStream
    .Distinct($"{TaskName}: exclude duplicate brokers", i => i.BrokerCode)
    .Select($"{TaskName}: create brokers", i => new Company
    {
        InternalCode = i.BrokerCode,
        Name = i.BrokerName
    })
    .EfCoreSave($"{TaskName}: Insert brokers", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", i => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var counterpartyRelationshipStream = brokerStream
    .Select($"{TaskName}: link currency to relationship", euroCurrency, (l, r) => new { BrokerId = l.Id, CurrencyId = r.Id })
    .EfCoreLookup($"{TaskName}: get related relationship in db", o => o
        .LeftJoinEntity(i => i.BrokerId, (CounterpartyRelationship i) => i.EntityId, (l, r) => r ?? new CounterpartyRelationship
        {
            CounterpartyType = CounterpartyType.Broker,
            EmirClassification = EmirClassification.Financial,
            EntityId = l.BrokerId,
            CurrencyId = l.CurrencyId
        })
        .Where(i => (i.StartDate <= DateTime.Now || i.StartDate == null) && (i.EndDate == null || i.EndDate > DateTime.Now))
        .CacheFullDataset())
    .EfCoreSaveCorrelated($"{TaskName}: save relationship", o => o.DoNotUpdateIfExists());

var transactionToSaveStream = transFileStream
    .EfCoreLookup($"{TaskName}: get related portfolio", o => o.LeftJoinEntity(i => i.Fund, (Portfolio i) => i.InternalCode, (l, r) => new { FileRow = l, Portfolio = r }).CacheFullDataset())
    .Where($"{TaskName}: exclude transaction with unfound portfolio", i => i.Portfolio != null)
    .CorrelateToSingle($"{TaskName}: get correlated relationship", counterpartyRelationshipStream, (l, r) => new { l.FileRow, l.Portfolio, Relationship = r });


var relationshipLinkStream = transactionToSaveStream
    .Distinct($"{TaskName}: distinct portfolio/broker relationship", i => new
    {
        PortfolioId = i.Portfolio.Id,
        RelationshipId = i.Relationship.Id
    })
    .Select($"{TaskName}: create relationship link", i => new RelationshipPortfolio
    {
        PortfolioId = i.Portfolio.Id,
        RelationshipId = i.Relationship.Id
    })
    .EfCoreSaveCorrelated($"{TaskName}: save relationship link", o => o.DoNotUpdateIfExists());

var savedOptionTransactionStream = transactionToSaveStream
    .LookupCurrency($"{TaskName}: get purchase currency", i => i.FileRow.PurchaseCcy, (l, r) => new { l.FileRow, l.Portfolio, l.Relationship, PurchaseCcy = r })
    .LookupCurrency($"{TaskName}: get sold currency", i => i.FileRow.SoldCcy, (l, r) => new { l.FileRow, l.Portfolio, l.Relationship, l.PurchaseCcy, SoldCcy = r })
    .Select($"{TaskName}: Create option transaction", i => CreateFxTransaction(
        i.Portfolio.Id,
        i.Relationship.Id,
        i.PurchaseCcy.Id,
        i.SoldCcy.Id,
        i.FileRow.Description,
        i.FileRow.NavDate,
        i.FileRow.Fund,
        i.FileRow.TradeDate,
        i.FileRow.ExchangeRate,
        i.FileRow.PurchaseCcy,
        i.FileRow.PurchasedAmount,
        i.FileRow.SettlementDate,
        i.FileRow.SoldAmount,
        i.FileRow.SoldCcy,
        i.FileRow.ContractNo,
        i.FileRow.BrokerCode
    ))
    .EfCoreSave($"{TaskName}: Save fx transaction", o => o.SeekOn(i => i.TransactionCode));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedOptionTransactionStream, relationshipLinkStream);

FxTransaction CreateFxTransaction(
    int portfolioId,
    int brokerRelationshipId,
    int purchaseCcyId,
    int soldCcyId,
    string description,
    DateTime navDate,
    string fund,
    DateTime tradeDate,
    double exchangeRate,
    string purchaseCcy,
    double purchasedAmount,
    DateTime settlementDate,
    double soldAmount,
    string soldCcy,
    string contractNo,
    string brokerCode)
{
    return new FxTransaction
    {
        BrokerId = brokerRelationshipId,
        Description = description,
        FxRate = exchangeRate,
        NavDate = navDate,
        PurchaseCurrencyId = purchaseCcyId,
        PurchaseAmount = purchasedAmount,
        SettlementDate = settlementDate,
        SoldAmount = soldAmount,
        SoldCurrencyId = soldCcyId,
        PortfolioId = portfolioId,
        TransactionCode = $"{fund}-{tradeDate:yyyyMMdd}-{contractNo}-{brokerCode}",
        TradeDate = tradeDate
    };
}
