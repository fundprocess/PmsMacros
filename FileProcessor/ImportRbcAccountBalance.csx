var rbcAccountBalanceFileDefinition = FlatFileDefinition.Create(i => new
{
    SubFundCode = i.ToColumn("SubFund Code"),
    AccountNumber = i.ToColumn("Account Number"),
    AccountName = i.ToColumn("Account Name"),
    NavDate = i.ToDateColumn("NAV Date", "yyyyMMdd"),
    AccountBalanceInAccountCurrency = i.ToNumberColumn<double>("Account Balance in Account Currency", "."),
    MarketValueAmountInFundCcy = i.ToNumberColumn<double>("Market Value Amount in Fund Ccy", "."),
    AccountCurrency = i.ToColumn("Account Currency"),
    NavVersion = i.ToColumn("NAV Version"),
    AccountingChartLabelLevel1 = i.ToColumn("Accounting Chart Label Level 1"),
    BalanceChartLabel = i.ToColumn("Balance Chart Label"),
    FileName = i.ToSourceName()
}).IsColumnSeparated(',');

DateTime MinDate(DateTime a, DateTime b) => a < b ? a : b;
DateTime MaxDate(DateTime a, DateTime b) => a > b ? a : b;

var classificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create RBC Accounting chart classification type", ctx => new MovementClassificationType { Code = "RbcAccountingChart", Name = new MultiCultureString { ["en"] = "Accounting chart RBC",["fr"] = "Tableau comptable RBC" } })
    .EfCoreSave($"{TaskName}: Save RBC Accounting chart classification type", o => o.SeekOn(i => i.Code))
    .EnsureSingle($"{TaskName}: only one classification type");

var fileRowStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse account balance file", rbcAccountBalanceFileDefinition, true)
    .Select($"{TaskName}: restructure file row", classificationTypeStream, (i, ct) => new
    {
        FileRow = i,
        Classification1 = new
        {
            ClassificationTypeId = ct.Id,
            Code = i.AccountingChartLabelLevel1.Substring(0, 1),
            Name = new MultiCultureString { ["en"] = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(i.AccountingChartLabelLevel1.Substring(1).ToLower()) }
        },
        Classification2 = new
        {
            ClassificationTypeId = ct.Id,
            Code = i.BalanceChartLabel.Substring(0, 6),
            Name = new MultiCultureString { ["en"] = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(i.BalanceChartLabel.Substring(6).ToLower()) }
        },
        Classification3 = new
        {
            ClassificationTypeId = ct.Id,
            Code = i.AccountNumber,
            Name = new MultiCultureString { ["en"] = i.AccountName }
        },
    })
    .EfCoreLookup($"{TaskName}: get related portfolio", o => o
        .Set<Portfolio>()
        .On(i => i.FileRow.SubFundCode, i => i.InternalCode)
        .Select((l, r) => new
        {
            l.FileRow,
            l.Classification1,
            l.Classification2,
            l.Classification3,
            Portfolio = r
        })
        .CacheFullDataset())
    .Where($"{TaskName}: Exclude account balance with no related portfolio", i => i.Portfolio != null);

var deletedScopeStream = fileRowStream
    .Aggregate($"{TaskName}: group row per file and portfolio", i => new { i.FileRow.FileName, i.Portfolio.Id }, i => new
    {
        MinDate = i.FileRow.NavDate,
        MaxDate = i.FileRow.NavDate
    }, (a, v) => new
    {
        MinDate = MinDate(v.FileRow.NavDate, a.MinDate),
        MaxDate = MaxDate(v.FileRow.NavDate, a.MinDate)
    })
    .Select($"{TaskName}: prepare data to remove", i =>
    (
        PortfolioId: i.Key.Id,
        MinDate: i.Aggregation.MinDate,
        MaxDate: i.Aggregation.MaxDate
    ))
    .EfCoreDelete($"{TaskName}: delete existing data in the scope", o => o
        .Set<AccountBalance>()
        .Where((i, a) => a.PortfolioId == i.PortfolioId && a.Date >= i.MinDate && a.Date <= i.MaxDate))
    .Select($"{TaskName}: change scope into object (technical reason)", i => new object());

#region Classifications
var classification1Stream = fileRowStream
    .Distinct($"{TaskName}: distinct classification 1", i => i.Classification1.Code)
    .Select($"{TaskName}: create classification 1", c => new MovementClassification
    {
        Name = c.Classification1.Name,
        Code = c.Classification1.Code,
        ClassificationTypeId = c.Classification1.ClassificationTypeId
    })
    .EfCoreSave($"{TaskName}: Insert level 1 classification", o => o.SeekOn(i => new { i.ClassificationTypeId, i.Code }).DoNotUpdateIfExists());

var classification2Stream = fileRowStream
    .Distinct($"{TaskName}: distinct classification 2", i => i.Classification2.Code)
    .Lookup($"{TaskName}: get related classification 1", classification1Stream, i => i.Classification1.Code, i => i.Code, (l, r) => new { l.Classification2, Classification1 = r })
    .Select($"{TaskName}: create classification 2", c => new MovementClassification
    {
        Name = c.Classification2.Name,
        Code = c.Classification2.Code,
        ClassificationTypeId = c.Classification2.ClassificationTypeId,
        ParentId = c.Classification1.Id
    })
    .EfCoreSave($"{TaskName}: Insert level 2 classification", o => o.SeekOn(i => new { i.ClassificationTypeId, i.Code }).DoNotUpdateIfExists());

var classification3Stream = fileRowStream
    .Distinct($"{TaskName}: distinct classification 3", i => i.Classification3.Code)
    .Where($"{TaskName}: keep classification that are different than their parent", i => i.Classification2.Code != i.Classification3.Code)
    .Lookup($"{TaskName}: get related classification 2", classification2Stream, i => i.Classification2.Code, i => i.Code, (l, r) => new { l.Classification3, Classification2 = r })
    .Select($"{TaskName}: create classification 3", c => new MovementClassification
    {
        Name = c.Classification3.Name,
        Code = c.Classification3.Code,
        ClassificationTypeId = c.Classification3.ClassificationTypeId,
        ParentId = c.Classification2.Id
    })
    .EfCoreSave($"{TaskName}: Insert level 3 classification", o => o.SeekOn(i => new { i.ClassificationTypeId, i.Code }).DoNotUpdateIfExists());

var classificationStream = classification2Stream.UnionAll($"{TaskName}: join classifications level 2 and 3", classification3Stream);
#endregion


var accountBalanceDataAfterExistingScopeDeletionStream = fileRowStream
    .WaitWhenDone($"{TaskName}: delay till the preexisting scope is deleted", deletedScopeStream)
    .LookupCurrency($"{TaskName}: get related currency", i => i.FileRow.AccountCurrency, (l, r) => new
    {
        l.FileRow,
        l.Portfolio,
        l.Classification3,
        Currency = r
    })
    .Lookup($"{TaskName}: get related classification", classificationStream, i => i.Classification3.Code, i => i.Code, (l, r) => new
    {
        l.Currency,
        l.Portfolio,
        l.FileRow,
        Classification = r
    });

var accountBalanceSaved = accountBalanceDataAfterExistingScopeDeletionStream
    .Select($"{TaskName}: create account balance", i => new
    {
        i.Classification,
        AccountBalance = new AccountBalance
        {
            PortfolioId = i.Portfolio.Id,
            Date = i.FileRow.NavDate,
            Balance = i.FileRow.AccountBalanceInAccountCurrency,
            BalanceInPortfolioCcy = i.FileRow.MarketValueAmountInFundCcy,
            CurrencyId = i.Currency.Id
        }
    })
    .EfCoreSave($"{TaskName}: save account balance", o => o.Entity(i => i.AccountBalance).InsertOnly().Output((i, e) => i))
    .Select($"{TaskName}: create classification for account balance", i => new ClassificationOfAccountBalance
    {
        AccountBalanceId = i.AccountBalance.Id,
        ClassificationId = i.Classification.Id,
        ClassificationTypeId = i.Classification.ClassificationTypeId
    })
    .EfCoreSave($"{TaskName}: Insert classification for balance", o => o.DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", accountBalanceSaved);