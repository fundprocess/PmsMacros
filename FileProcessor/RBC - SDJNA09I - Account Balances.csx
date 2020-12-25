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
}).IsColumnSeparated(';');

var classificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create RBC Accounting chart classification type", ctx => new MovementClassificationType { Code = "RbcAccountingChart", Name = new MultiCultureString { ["en"] = "Accounting chart RBC",["fr"] = "Tableau comptable RBC" } })
    .EfCoreSave($"{TaskName}: Save RBC Accounting chart classification type", o => o.SeekOn(i => i.Code))
    .EnsureSingle($"{TaskName}: Only one classification type");

var fileRowStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse account balance file", rbcAccountBalanceFileDefinition, true)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var accountingDayStream = fileRowStream
    .Distinct($"{TaskName}: Distinct accounting days", i => new { i.NavDate, i.SubFundCode })
    .LookupPortfolio($"{TaskName}: Get Related portfolio", i => i.SubFundCode, (l,r) => new AccountingDay
    {
        PortfolioId = r.Id,
        Date = l.NavDate,
    })
    .EfCoreSave($"{TaskName}: Save Accounting Day", o => o
        .SeekOn(i => new { i.Date, i.PortfolioId })
        .DoNotUpdateIfExists())
    .EfCoreDelete($"{TaskName}: Delete related balances", o => o
        .Set<AccountBalance>()
        .Where((i, a) => a.AccountingDayId == i.Id));

#region Classifications
var classificationRowStream = fileRowStream
    .Select($"{TaskName}: Get classification type", classificationTypeStream, (i,ct) => new 
    {
        Level1Code = i.AccountingChartLabelLevel1.Substring(0, 1),
        Level1Name = new MultiCultureString { ["en"] = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(i.AccountingChartLabelLevel1.Substring(1).ToLower()) },
        Level2Code = i.BalanceChartLabel.Substring(0, 6),
        Level2Name = new MultiCultureString { ["en"] = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(i.BalanceChartLabel.Substring(6).ToLower()) },
        Level3Code = i.AccountNumber,
        Level3Name = new MultiCultureString { ["en"] = i.AccountName },
        ClassificationTypeId = ct.Id
    });

var classification1Stream = classificationRowStream
    .Distinct($"{TaskName}: Distinct classification 1", i => i.Level1Code)
    .Select($"{TaskName}: Create classification 1", i => new MovementClassification
    {
        Code = i.Level1Code,
        Name = i.Level1Name,
        ClassificationTypeId = i.ClassificationTypeId,
    })
    .EfCoreSave($"{TaskName}: Save classification 1", o => o
        .SeekOn(i => i.Code)
        .DoNotUpdateIfExists());

var classification2Stream = classificationRowStream
    .Distinct($"{TaskName}: Distinct classification 2", i => i.Level2Code)
    .CorrelateToSingle($"{TaskName}: Get related classification 1", classification1Stream, (i,c) => new MovementClassification
    {
        Code = i.Level2Code,
        Name = i.Level2Name,
        ClassificationTypeId = i.ClassificationTypeId,
        ParentId = c.Id
    })
    .EfCoreSave($"{TaskName}: Save classification 2", o => o
        .SeekOn(i => i.Code)
        .DoNotUpdateIfExists());

var classification3Stream = classificationRowStream
    .Distinct($"{TaskName}: Distinct classification 3", i => i.Level3Code)
    .CorrelateToSingle($"{TaskName}: Get related classification 2", classification2Stream, (i,c) => new MovementClassification
    {
        Code = i.Level3Code,
        Name = i.Level3Name,
        ClassificationTypeId = i.ClassificationTypeId,
        ParentId = c.Id
    })
    .EfCoreSave($"{TaskName}: Save classification 3", o => o
        .SeekOn(i => i.Code)
        .DoNotUpdateIfExists());

#endregion

var accountBalanceSaved = fileRowStream
    .LookupCurrency($"{TaskName}: get related currency", i => i.AccountCurrency, (l, r) => new
    {
        FileRow=l,
        Currency = r
    })
    .CorrelateToSingle($"{TaskName}: get related accounting day", accountingDayStream, (l, r) => new AccountBalance
    {
        AccountingDayId = r.Id,
        Balance = l.FileRow.AccountBalanceInAccountCurrency,
        BalanceInPortfolioCcy = l.FileRow.MarketValueAmountInFundCcy,
        CurrencyId = l.Currency.Id
    })
    .EfCoreSaveCorrelated($"{TaskName}: Save account balance", o => o.InsertOnly());

var classificationOfAccount = accountBalanceSaved.CorrelateToSingle($"{TaskName}: Get related classifications", classification3Stream, (a, c) => new ClassificationOfAccountBalance
    {
        AccountBalanceId = a.Id,
        ClassificationId = c.Id,
        ClassificationTypeId = c.ClassificationTypeId
    })
    .EfCoreSaveCorrelated($"{TaskName}: Insert classification for balance", o => o.DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", classificationOfAccount);
