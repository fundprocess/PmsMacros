var transactionsFileDefinition = FlatFileDefinition.Create(i => new
{
    TransactionCode = i.ToColumn("Code"),
    BrokerName = i.ToColumn("Broker Name"),
    Description = i.ToColumn("Description"),
    NavDate = i.ToDateColumn("NAV Date", "yyyyMMdd"),
    TradeDate = i.ToDateColumn("Trade Date", "yyyyMMdd"),
    PortfolioCode = i.ToColumn("Portfolio Code"),
    PortfolioName = i.ToColumn("Portfolio Name"),
    FeesInSecurityCcy= i.ToNumberColumn<double?>("Fees In Security Ccy", "."),
    GrossAmountInPortfolioCcy= i.ToNumberColumn<double?>("Gross Amount In Portfolio Ccy", "."),
    GrossAmountInSecurityCcy= i.ToNumberColumn<double>("Gross Amount In Security Ccy", "."),
    NetAmountInPortfolioCcy= i.ToNumberColumn<double?>("Net Amount In Portfolio Ccy", "."),
    NetAmountInSecurityCcy= i.ToNumberColumn<double?>("Net Amount In Security Ccy", "."),
    OperationType= i.ToColumn("Operation Type"),
    PriceInSecurityCcy= i.ToNumberColumn<double>("Price In Security Ccy", "."),
    Quantity= i.ToNumberColumn<double>("Quantity", "."),
    SecurityIsin= i.ToColumn("Security Isin"),
    SecurityCode= i.ToColumn("Security Code"),
    SecurityName= i.ToColumn("Security Name"),
    TransactionType= i.ToColumn("Transaction Type"),
    ValueDate= i.ToDateColumn("Value Date", "yyyyMMdd"),
}).IsColumnSeparated(',');
var lastYear=DateTime.Now.AddYears(-1);
return ProcessContextStream
    .EfCoreSelect($"{TaskName}: get transactions of the past year", 
        o=>o.Set<SecurityTransaction>()
            .Include(t=>t.Portfolio)
            .Include(t=>t.Broker).ThenInclude(b=>b.Entity)
            .Include(t=>t.Security)
            .Where(t=>t.TradeDate >= lastYear))
    .Select($"{TaskName}: create row to save", i=> new {
        TransactionCode = i.TransactionCode,
        BrokerName = GetEntityName(i.Broker?.Entity),
        Description = i.Description,
        NavDate = i.NavDate,
        TradeDate = i.TradeDate,
        PortfolioCode = i.Portfolio.InternalCode,
        PortfolioName = i.Portfolio.Name,
        FeesInSecurityCcy= (double?)i.FeesInSecurityCcy,
        GrossAmountInPortfolioCcy= (double?)i.GrossAmountInPortfolioCcy,
        GrossAmountInSecurityCcy= (double)i.GrossAmountInSecurityCcy,
        NetAmountInPortfolioCcy= (double?)i.NetAmountInPortfolioCcy,
        NetAmountInSecurityCcy= (double?)i.NetAmountInSecurityCcy,
        OperationType= i.OperationType.ToString(),
        PriceInSecurityCcy= (double)i.PriceInSecurityCcy,
        Quantity= i.Quantity,
        SecurityIsin= (i.Security as SecurityInstrument)?.Isin,
        SecurityCode= i.Security.InternalCode,
        SecurityName= i.Security.Name,
        TransactionType= i.TransactionType.ToString(),
        ValueDate= i.ValueDate,
    })
    .ToTextFileValue($"{TaskName}: save into text file value", $"transactions{DateTime.Now:yyyyMMddhhmmss}.csv", transactionsFileDefinition);
string GetEntityName(Entity entity){
    switch(entity){
        case Person person: return $"{person.FirstName} {person.LastName}";
        case Company company: return company.Name;
    }
    return null;
}