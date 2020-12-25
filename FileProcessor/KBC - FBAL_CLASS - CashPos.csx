//FILE MAPPING
var kbcPositionFileDefinition = FlatFileDefinition.Create(i => new
{
    ClientCode = i.ToColumn<string>("CLIENT NBR"),
    ClientName = i.ToColumn<string>("CLIENT NAME"),
    SecurityInternalCode = i.ToColumn<string>("ACCOUNT NBR"), 
    Currency = i.ToColumn<string>("CUR"),
    Description = i.ToColumn<string>("DESCRIPTION"),
    Date  =   i.ToDateColumn("GEN DATE", "yyyyMMdd"),
    //MarketValueInSecurityCcy =
    MarketValueInPortfolioCcy = i.ToNumberColumn<double?>("BALANCE", "."),
    FileName = i.ToSourceName()
}).IsColumnSeparated(',');

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse position file", kbcPositionFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//CREATE PORTFOLIOS
var euroCurrencyStream = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", i => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var portfolioStream = posFileStream
    .Distinct($"{TaskName}: Distinct portfolios", i => i.ClientCode + "-KBC")
    .Select($"{TaskName}: Create portfolios", euroCurrencyStream, (l, r) => new DiscretionaryPortfolio
    {
        InternalCode = l.ClientCode +"-KBC",
        Name = l.ClientName,
        ShortName = "KBC_"+l.ClientCode,
        CurrencyId = r.Id, //!! check if all client portfolio in EUR
        InceptionDate = DateTime.Today,
        PricingFrequency = FrequencyType.Daily
    })
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//OVERWRITE PORTFOLIO NAME
var portfolioNameStream = portfolioStream
    .CorrelateToSingle($"{TaskName}: Fixing portfolio name - get related line in file", posFileStream, (l, r) => new { portfolio = l, fileRow = r })
    .Fix($"{TaskName}: Fixing portfolio name", i => i.FixProperty(j => j.portfolio.Name).AlwaysWith(j => j.portfolio.Name = j.fileRow.ClientName))
    .EfCoreSave("Fixing portfolio name - Save");
    //.Select($"{TaskName}: Fix portfolio name", i => {i.portfolio.Name = i.fileRow.ClientName; return i.portfolio; })
    //.EfCoreSave($"{TaskName}: save portfolio name",o=>o.WithMode(SaveMode.EntityFrameworkCore) );

//CREATE INVESTOR PERSONS
var individualStream = posFileStream  //TODO: ! Add where on client nature=person
    .Distinct($"{TaskName}: Distinct on individuals", i => i.ClientCode)
    .Select($"{TaskName}: Create person entity",euroCurrencyStream, (i,j) => new Person
    {
        InternalCode = i.ClientCode+"-KBC",
        LastName = i.ClientName,
        CurrencyId = j.Id, //!! check if all client portfolio in EUR
        Culture = new CultureInfo("en-GB"),
    })
    .EfCoreSave($"{TaskName}: Insert client person", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//CREATE INVESTOR PERSON RELATIONSHIPS
var individualInvestorRelationshipStream = individualStream
    .Select($"{TaskName} create individual Investor Relationship",euroCurrencyStream, (i,j)=>new InvestorRelationship{
        InvestorType = InvestorType.Retail,
        //PrimaryInternalAdvisorId=
        StatementFrequency = FrequencyType.Quarterly,
        EntityId = i.Id,
        StartDate = DateTime.Today,
        CurrencyId = j.Id,
    })
    .EfCoreSave($"{TaskName}: Save Individual Investor Relationship", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());

//CREATE INVESTOR COMPANIES + RELATIONSHIPS
//TODO: ...


//CREATE TARGET CASH SECURITIES
var targetCashSecurityStream = posFileStream
        .Distinct($"{TaskName}: distinct cash securities", i => i.ClientCode + "-" +i.SecurityInternalCode)
        .LookupCurrency($"{TaskName}: get related currency", l => l.Currency, (l, r) => new { FileRow = l, Currency = r })
        .Select($"{TaskName}: create CASH security", i => new Cash{
                    InternalCode = i.FileRow.ClientCode + "-" + i.FileRow.SecurityInternalCode,
                    CurrencyId = i.Currency.Id,
                    Name = i.FileRow.ClientName+ "-" + i.FileRow.Description,
                    ShortName = i.FileRow.SecurityInternalCode
                })
        .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var portfolioCompositionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => 
            new PortfolioComposition { Date = l.Date, PortfolioId = r.Id })
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioId, i.Date }, true)
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get related cash security for position", targetCashSecurityStream, (l, r) => new { FileRow = l, CashSecurity = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r) 
                        => new { fileRow = l.FileRow, cashSecurity = l.CashSecurity, compo = r })
    .Select($"{TaskName}: create position", i => new Position
    {
        SecurityId = i.cashSecurity.Id,
        PortfolioCompositionId = i.compo.Id,
        Value = 1, //(It's the quantity)
        MarketValueInPortfolioCcy = i.fileRow.MarketValueInPortfolioCcy.Value, //Account Balance in portfolio CCy
        // MarketValueInSecurityCcy = i.fileRow.MarketValueInSecurityCcy, //Account Balance in account CCy
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));
return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream,portfolioNameStream);

