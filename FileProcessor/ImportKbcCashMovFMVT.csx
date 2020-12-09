//1. FILE COLUMN MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn("CLIENT NBR"),
    Currency = i.ToColumn("CUR"),
    TradeDate = i.ToDateColumn("TRADING DATE", "yyyyMMdd"),
    ValueDate = i.ToDateColumn("VALUE DATE", "yyyyMMdd"),
    Description =i.ToColumn("DESCRIPTION"),
    GrossAmountInSecurityCcy = i.ToNumberColumn<double>("AMOUNT", "."),
    AccountNbr = i.ToColumn("ACCOUNT NBR"),
    SequenceNbr = i.ToNumberColumn<int?>("SEQ NBR", "."),
   
}).IsColumnSeparated(',');

var movFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse mov file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//2. CREATE PORTFOLIOS
// Portfolio
var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", i => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var portfolioStream = movFileStream
    .Distinct($"{TaskName}: Distinct portfolios", i => i.PortfolioCode)
    .Select($"{TaskName}: Create portfolios", euroCurrency, (i,cur) => new DiscretionaryPortfolio(){
        InternalCode = i.PortfolioCode,
        Name = i.PortfolioCode,
        ShortName = "KBC",
        CurrencyId = cur.Id,
        PricingFrequency = FrequencyType.Daily})
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var savedMovStream = movFileStream
    .LookupCurrency($"{TaskName}: Get related currency for portfolio", l => l.Currency, (l, r) => new { FileRow = l, Currency = r })
    .LookupPortfolio($"{TaskName}: Lookup for portfolio", i => i.FileRow.PortfolioCode, (l, r) 
                    => new { FileRow = l.FileRow,Currency = l.Currency, Portfolio = r }) 
    .Select($"{TaskName}: Create new movement", i => new CashMovement
    {
        PortfolioId = i.Portfolio.Id,
        CurrencyId = i.Currency.Id,
        TradeDate = i.FileRow.TradeDate,
        ValueDate = i.FileRow.ValueDate,
        Description = i.FileRow.Description,
        GrossAmountInSecurityCcy = i.FileRow.GrossAmountInSecurityCcy,
        TransactionCode = $"{i.FileRow.PortfolioCode}-{i.FileRow.AccountNbr}-{i.FileRow.TradeDate:yyyyMMdd}",
        MovementCode = i.FileRow.AccountNbr + " - "+ i.FileRow.SequenceNbr, 
    }).EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());


return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedMovStream);
