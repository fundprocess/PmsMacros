// Portfolio -> People|Company -> IndivInvestor|companyinvestor -> RelationshipPortfolio
// InvestorClassifications (MiFID Class)
// Target security: Issuer + Classifications
// Cash Mov classification
// Compute weights

//1. FILE MAPPING
var kbcSecPosFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioInternalCode = i.ToColumn<string>("CLIENT NBR"),
    Isin = i.ToColumn<string>("ISIN"),
    BbgCode = i.ToColumn<string>("BB TICKER CODE"),
    SecInternalCode = i.ToColumn<string>("SEC NBR"),
    SecName = i.ToColumn<string>("SEC NAME"),
    SecCur = i.ToColumn<string>("SEC CUR"),
    SecType = i.ToColumn<string>("SEC TYPE"),
    Date  =   i.ToDateColumn("GEN DATE", "yyyyMMdd"),
    Quantity = i.ToNumberColumn<double>("POSITION", "."),
    MarketValueInSecurityCcy = i.ToNumberColumn<double?>("VALUE SEC INT IN", "."),    
    MarketValueInPortfolioCcy  = i.ToNumberColumn<double?>("VAL PORT INT INC", "."),
    FxRate =  i.ToNumberColumn<double?>("EXCH RATE", "."),  
    Price  =  i.ToNumberColumn<double>("PRICE", "."),  
    PriceDate  = i.ToDateColumn("PRICE DATE", "yyyyMMdd"),
   
   //! CLI_TYPE, CLI_NATURE, CLI_STSRTG, POS_AVGPRC, POS_RESULT, SEC_HOLDER, SEC_BBEXCH 

}).IsColumnSeparated(',');

var kbcCashPosFileDefinition2 = FlatFileDefinition.Create(i => new
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

var secPosFileStream = FileStream
    .Where($"{TaskName} Only Security positions file",i => i.Name.ToLower().Contains("posiall"))
    .CrossApplyTextFile($"{TaskName}: parse position file", kbcSecPosFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

... TODO ....
LEFT JOIN

//.FixProperty(p => p.PosDate).IfNullWith(i => (!DateTime.TryParseExact(i.FileName.Substring(0, 10), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date) ? (DateTime?)null : date)))

#region PORTFOLIOS / PERSONS-COMPANIES / INVESTOR RELATIONSHIPS

var euroCurrencyStream = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx,i) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

string GetPortfolioInternalCode(string clientNbr)
    => clientNbr + "-KBC";

var portfolioFileStream = secPosFileStream.Distinct($"{TaskName}: Distinct portfolios", i => i.PortfolioInternalCode);

var portfolioStream = portfolioFileStream
    .Select($"{TaskName}: Create portfolios", euroCurrencyStream, (l, r) => new DiscretionaryPortfolio
    {
        InternalCode = GetPortfolioInternalCode(l.PortfolioInternalCode),
        Name = l.PortfolioInternalCode,
        ShortName = l.PortfolioInternalCode,
        CurrencyId = r.Id,
        InceptionDate = DateTime.Today,
        PricingFrequency = FrequencyType.Daily
    })    
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var peopleStream = portfolioFileStream
    //.Where($"{TaskName}: filter individual client", i=>i.InitMifid.Contains("(01)")) //TODO: add when KBC ready
    //.LookupCountry($"{TaskName}: get related country for person", l => l.Domicile, (l, r) => new {fileRow = l, CountryId = r?.Id})
    // .LookupCurrency($"{TaskName}: get related currency for person", l => l.fileRow.DefaultCcy, (l, r) => 
    //             new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = r?.Id})
    .Select($"{TaskName}: create person entity", i => new Person
    {
        InternalCode = GetPortfolioInternalCode(i.PortfolioInternalCode),
        FirstName = "",
        LastName = $"{i.PortfolioInternalCode}",
        // CurrencyId = i.CurrencyId.HasValue? i.CurrencyId.Value:(int?)null,
        // CountryId = i.CountryId.HasValue? i.CountryId.Value:(int?)null,
        Culture = new CultureInfo("en-GB"),
    })
    .EfCoreSave($"{TaskName}: save person", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var individualInvestorRelationshipStream = peopleStream
    .Select($"{TaskName} create individual Investor Relationship", i => 
        new {EntityInternalCode=i.InternalCode, Relationship= new InvestorRelationship{
        EntityId = i.Id,
        InvestorType = InvestorType.Retail,
        StatementFrequency = FrequencyType.Quarterly,
        StartDate = DateTime.Today,
        CurrencyId = i.CurrencyId.Value,
    }})
    .EfCoreSave($"{TaskName}: Save Individual Investor Relationship", o => o
        .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));
        
// var companyStream = filePortfoliosStream  //TODO: add when KBC ready!
//     .Where($"{TaskName}: filter company client", i=> !i.InitMifid.Contains("(01)"))
//     .LookupCountry($"{TaskName}: get related country for companies", l => l.Domicile, (l, r) => new {fileRow = l, CountryId = r?.Id})
//     .LookupCurrency($"{TaskName}: get related currency for companies", l => l.fileRow.DefaultCcy, (l, r) => 
//                 new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = r?.Id})
//     .Select($"{TaskName}: create client company entity", i => new Company
//     {
//         InternalCode = GetPortfolioInternalCode(i.fileRow.ContId),
//         Name = $"{i.fileRow.ContId}",
//         CurrencyId = i.CurrencyId.HasValue? i.CurrencyId.Value:(int?)null,
//         CountryId = i.CountryId.HasValue? i.CountryId.Value:(int?)null,
//         Culture = new CultureInfo("en-GB"),
//         YearEnd = new DateOfYear(12,31)
//     }).EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// var companyInvestorRelationshipStream = companyStream
//     .CorrelateToSingle($"{TaskName}: get related company  file row", filePortfoliosStream, (l, r) => new { Company = l, FileRow=r})
//     .Select($"{TaskName} create company Investor Relationship", i =>
//      new {EntityInternalCode=i.Company.InternalCode, Relationship= new InvestorRelationship{
//         EntityId = i.Company.Id,
//         InvestorType = i.FileRow.InitMifid.Contains("(03)")? InvestorType.Institutional:InvestorType.Retail,
//         StatementFrequency = FrequencyType.Quarterly,
//         StartDate = i.FileRow.OpenDate,
//         CurrencyId = i.Company.CurrencyId.Value,
//     }})
//     .EfCoreSave($"{TaskName}: Save Company Investor Relationship", o => o
//         .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));

var investorsStream = individualInvestorRelationshipStream;
    //.Union($"{TaskName}: merge of the investor relationship streams", companyInvestorRelationshipStream); //TODO: add when KBC ready!

var relationshipPortfoliosStream = investorsStream
	.Lookup($"{TaskName}: Link Investor-Porfolio - get related portfolio",portfolioStream, 
                i=>i.EntityInternalCode,i=>i.InternalCode, (l,r) => new {InvestorRelationship = l.Relationship, Portfolio = r})
	.Where($"{TaskName}: Link Investor-Porfolio - Filter existing portfolio", i=>i.Portfolio !=null)
    .Select($"{TaskName}: create link between investor and related portfolio", i => new RelationshipPortfolio {
		RelationshipId = i.InvestorRelationship.Id, PortfolioId=i.Portfolio.Id})
	.EfCoreSave($"{TaskName}: Save link Relationship-Portfolio", 
        o => o.SeekOn(i => new {i.RelationshipId, PortfolioId=i.PortfolioId}).DoNotUpdateIfExists());


#endregion


//2. CREATE TARGET SECURITIES
var targetSecurityStream = secPosFileStream
        .Distinct($"{TaskName}: distinct position securities", i => i.SecInternalCode)
        .LookupCurrency($"{TaskName}: get related currency", l => l.SecCur, (l, r) => new { FileRow = l, Currency = r })
        //CreateSecurity(string internalCode, string secType, string secName, int? currencyId, string isin)
        .Select($"{TaskName}: create target security", i => CreateSecurity(i.FileRow.SecInternalCode,i.FileRow.SecType,
                        i.FileRow.SecName,i.Currency?.Id,i.FileRow.Isin) )
        .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());


var portfolioCompositionStream = secPosFileStream
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => 
            new PortfolioComposition { Date = l.Date, PortfolioId = r.Id })
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioId, i.Date }, true)
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = secPosFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r) 
                        => new { fileRow = l.FileRow, sec = l.Security, compo = r })
    .Select($"{TaskName}: create position", i => new Position
    {
        SecurityId = i.sec.Id,
        PortfolioCompositionId = i.compo.Id,
        MarketValueInPortfolioCcy = i.fileRow.MarketValueInPortfolioCcy.Value,
        MarketValueInSecurityCcy = i.fileRow.MarketValueInSecurityCcy,
        Value = i.fileRow.Quantity,
        ValuationPrice = i.fileRow.Price,
        //CostPrice
        //BookCostInSecurityCcy
        //BookCostInPortfolioCcy
        //ProfitLossOnMarketPortfolioCcy
        //ProfitLossOnFxPortfolioCcy
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));
return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream);


Security CreateSecurity(string internalCode, string secType, string secName, int? currencyId, string isin)
{
    if (string.IsNullOrWhiteSpace(secType) && !string.IsNullOrWhiteSpace(isin)) secType = "FUND";
    Security security = null;
    switch (secType)
    {
        case "FUND":
            security = new ShareClass();
            break;
        case "TRACKER":
            security = new Etf();
            break;
        case "BOND":
            security = new Bond();
            break;
        case "SHARE":
            security = new Equity();
            break;
        case "RIGHT":
        case "COUPON":
            security = new Cash();
            break;
    }

    if (security != null)
    {
        security.InternalCode = (!string.IsNullOrEmpty(isin))?isin:internalCode;
        security.CurrencyId = currencyId;
        if (security is OptionFuture der)
            der.UnderlyingIsin = isin;
        else if (security is SecurityInstrument securityInstrument)
            securityInstrument.Isin = isin;
        security.Name = secName;
        security.ShortName = secName.Truncate(MaxLengths.ShortName);
    }

    return security;
}
