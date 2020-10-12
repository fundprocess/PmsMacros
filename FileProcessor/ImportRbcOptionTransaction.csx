var rbcTransactionFileDefinition = FlatFileDefinition.Create(i => new
{
    CounterpartyBrokerCode = i.ToColumn("Counterparty/Broker Code"),
    CounterpartyBrokerDescription = i.ToColumn("Counterparty/Broker Description"),
    OptionDescription = i.ToColumn("Option Description"),
    AccountingDate = i.ToDateColumn("Accounting Date", "yyyyMMdd"),
    FundCode = i.ToColumn("Fund Code"),
    TradeDate = i.ToDateColumn("Trade Date", "yyyyMMdd"),
    ContractNumber = i.ToColumn("Contract Number"),
    FeesAmountInTransactionCcy = i.ToNumberColumn<double?>("Fees Amount in Transaction Ccy", "."),
    CommissionAmount = i.ToNumberColumn<double?>("Commission Amount", "."),
    TradeAmount = i.ToNumberColumn<double>("Trade Amount", "."),
    Premium = i.ToNumberColumn<double>("Premium", "."),
    Quantity = i.ToNumberColumn<double>("Quantity", "."),
    OptionRbcdisCode = i.ToColumn("Option RBCDIS Code"),
    ValueDate = i.ToDateColumn("Value Date", "yyyyMMdd"),
    FileName = i.ToSourceName()
}).IsColumnSeparated(',');

var transFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse transaction file", rbcTransactionFileDefinition)
    .SetForCorrelation($"{TaskName}: Prepare correlation");

var brokerStream = transFileStream
    .Distinct($"{TaskName}: Exclude duplicate brokers", i => i.CounterpartyBrokerCode)
    .Select($"{TaskName}: Create brokers", i => new Company
    {
        InternalCode = i.CounterpartyBrokerCode,
        Name = i.CounterpartyBrokerDescription
    })
    .EfCoreSave($"{TaskName}: Insert brokers", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: Get euroCurrency", i => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: Ensures only one euro currency");

var counterpartyRelationshipStream = brokerStream
    .Select($"{TaskName}: Link currency to relationship", euroCurrency, (l, r) => new { BrokerId = l.Id, CurrencyId = r.Id })
    .EfCoreLookup($"{TaskName}: Get related relationship in db", o => o
        .Set<CounterpartyRelationship>()
        .Where(i => (i.StartDate <= DateTime.Now || i.StartDate == null) && (i.EndDate == null || i.EndDate > DateTime.Now))
        .On(i => i.BrokerId, i => i.EntityId)
        .Select((l, r) => r ?? new CounterpartyRelationship
        {
            CounterpartyType = CounterpartyType.Broker,
            EmirClassification = EmirClassification.Financial,
            EntityId = l.BrokerId,
            CurrencyId = l.CurrencyId
        })
        .CacheFullDataset())
    .EfCoreSaveCorrelated($"{TaskName}: Insert relationship", o => o.DoNotUpdateIfExists());

var transactionToSaveStream = transFileStream
    .Distinct($"{TaskName}: Distinct",
        i => new { i.FileName, i.TradeDate, i.CounterpartyBrokerCode, i.ContractNumber },
        o => o
            .ForProperty(i => i.Quantity, DistinctAggregator.Last)
            .ForProperty(i => i.FeesAmountInTransactionCcy, DistinctAggregator.Last)
            .ForProperty(i => i.CommissionAmount, DistinctAggregator.Last)
            .ForProperty(i => i.TradeAmount, DistinctAggregator.Last) //!!!!!!!!!!!!!!!!!!!
            .ForProperty(i => i.Premium, DistinctAggregator.Last) //!!!!!!!!!!!!!!!!!!!!
            )
    .EfCoreLookup($"{TaskName}: Get related portfolio", o => o
        .Set<Portfolio>()
        .On(i => i.FundCode, i => i.InternalCode)
        .Select((l, r) => new { FileRow = l, Portfolio = r })
        .CacheFullDataset())
    .Where($"{TaskName}: Exclude transaction unfound portfolio", i => i.Portfolio != null)
    .CorrelateToSingle($"{TaskName}: Get correlated relationship", counterpartyRelationshipStream, (l, r) => new { l.FileRow, l.Portfolio, Relationship = r });

var relationshipLinkStream = transactionToSaveStream
    .Distinct($"{TaskName}: Distinct portfolio/broker relationship", i => new
    {
        PortfolioId = i.Portfolio.Id,
        RelationshipId = i.Relationship.Id
    })
    .Select($"{TaskName}: Create relationship link", i => new RelationshipPortfolio
    {
        PortfolioId = i.Portfolio.Id,
        RelationshipId = i.Relationship.Id
    })
    .EfCoreSaveCorrelated($"{TaskName}: Insert relationship link", o => o.DoNotUpdateIfExists());

var savedTransactionStream = transactionToSaveStream
    .EfCoreLookup($"{TaskName}: Get target option by internal code", o => o
        .Set<Option>()
        .On(i => i.FileRow.OptionRbcdisCode, i => i.InternalCode)
        .Select((l, r) => new { l.FileRow, l.Portfolio, l.Relationship, TargetOption = r })
        .CacheFullDataset())
    .Where($"{TaskName}: Exclude movements with target option not found", i => i.TargetOption != null) // TODO: Check why not everything matches
    .Select($"{TaskName}: Create option transaction", i => CreateSecurityTransaction(
        i.Portfolio.Id,
        i.TargetOption.Id,
        i.Relationship.Id,
        i.FileRow.CounterpartyBrokerCode,
        i.FileRow.OptionDescription,
        i.FileRow.AccountingDate,
        i.FileRow.TradeDate,
        i.FileRow.ContractNumber,
        i.FileRow.FeesAmountInTransactionCcy,
        i.FileRow.CommissionAmount,
        i.FileRow.TradeAmount,
        i.FileRow.Premium,
        i.FileRow.Quantity,
        i.FileRow.ValueDate
    ))
    .EfCoreSave($"{TaskName}: Save option transaction", o => o.SeekOn(i => i.TransactionCode));
return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedTransactionStream, relationshipLinkStream);

SecurityTransaction CreateSecurityTransaction(
    int portfolioId,
    int optionId,
    int brokerId,
    string counterpartyBrokerCode,
    string optionDescription,
    DateTime accountingDate,
    DateTime tradeDate,
    string contractNumber,
    double? feesAmountInTransactionCcy,
    double? commissionAmount,
    double tradeAmount,
    double premium,
    double quantity,
    DateTime valueDate
)
{
    return new SecurityTransaction
    {
        PortfolioId = portfolioId,
        SecurityId = optionId,
        BrokerId = brokerId,
        OperationType = GetOperationType(optionDescription),
        TransactionType = TransactionType.SecurityMovement,
        Description = optionDescription,
        TradeDate = tradeDate,
        ValueDate = valueDate,
        NavDate = accountingDate,
        Quantity = quantity,
        PriceInSecurityCcy = premium,
        FeesInSecurityCcy = feesAmountInTransactionCcy == null && commissionAmount == null ? (double?)null : feesAmountInTransactionCcy ?? 0 + commissionAmount ?? 0,
        GrossAmountInSecurityCcy = tradeAmount,
        NetAmountInSecurityCcy = tradeAmount - feesAmountInTransactionCcy,
        TransactionCode = $"{tradeDate:yyyyMMdd}-{counterpartyBrokerCode}-{contractNumber}"
    };
}

OperationType GetOperationType(string optionDescription)
{
    if (optionDescription.StartsWith("purchase", true, null)) return OperationType.Purchase;
    if (optionDescription.StartsWith("sale", true, null)) return OperationType.Sale;
    if (optionDescription.StartsWith("closing", true, null)) return OperationType.Closing;
    throw new Exception("Impossible to define the operation type");
}
