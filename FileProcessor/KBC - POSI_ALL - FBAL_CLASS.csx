//1. FILE MAPPING
var fileDefinition = FlatFileDefinition.Create(i => new
{
    AccountNbr = i.ToColumn<string>("ACCOUNT NBR"),
    AveragePrice= i.ToNumberColumn<double>("AVERAGE PRICE", "."),
    Balance= i.ToNumberColumn<double>("BALANCE", "."),
    BalanceXxx= i.ToNumberColumn<double>("BALANCE XXX", "."),
    BbExchangeCode = i.ToColumn<string>("BB EXCHANGE CODE"),
    BbTickerCode = i.ToColumn<string>("BB TICKER CODE"),
    ClientName = i.ToColumn<string>("CLIENT NAME"),
    ClientNature = i.ToColumn<string>("CLIENT NATURE"),
    ClientNbr = i.ToColumn<string>("CLIENT NBR"),
    ClientType = i.ToColumn<string>("CLIENT TYPE"),
    Cur = i.ToColumn<string>("CUR"),
    Description = i.ToColumn<string>("DESCRIPTION"),
    Dos = i.ToColumn<string>("DOS"),
    ExchRate = i.ToNumberColumn<double>("EXCH RATE", "."),
    GenDate = i.ToDateColumn("GEN DATE", "yyyyMMdd"),
    Isin = i.ToColumn<string>("ISIN"),
    LastModifDate = i.ToOptionalDateColumn("LAST MODIF DATE", "yyyyMMdd"),
    MarketCode = i.ToColumn<string>("MARKET CODE"),
    MarketName = i.ToColumn<string>("MARKET NAME"),
    Pos_Date= i.ToOptionalDateColumn("POS_DATE", "yyyyMMdd"),
    Position= i.ToNumberColumn<double>("POSITION", "."),
    Price= i.ToNumberColumn<double>("PRICE", "."),
    PriceDate= i.ToOptionalDateColumn("PRICE DATE", "yyyyMMdd"),
    Result = i.ToColumn<string>("RESULT"),
    SecCur = i.ToColumn<string>("SEC CUR"),
    SecHolder = i.ToColumn<string>("SEC HOLDER"),
    SecName = i.ToColumn<string>("SEC NAME"),
    SecNbr = i.ToColumn<string>("SEC NBR"),
    SecType = i.ToColumn<string>("SEC TYPE"),
    StatusRating = i.ToColumn<string>("STATUS RATING"),
    TisDate = i.ToOptionalDateColumn("TIS DATE", "yyyyMMdd"),
    ValPortIntInc = i.ToNumberColumn<double>("VAL PORT INT INC","."),
    ValueSecIntIn = i.ToNumberColumn<double>("VALUE SEC INT IN","."),
    FilePath = i.ToSourceName(),
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var allPositionsFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse position file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var posiallStream = allPositionsFileStream.Where($"{TaskName} where POSI_ALL",i => i.FilePath.Contains("POSI_ALL"));
var fbalStream = allPositionsFileStream.Where($"{TaskName} where fbal",i => i.FilePath.Contains("FBAL_CLASS"));
     
var euroCurrencyStream = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx,i) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

string GetKbcClientCode(string clientNbr)
    => clientNbr + "-KBC";

var portfolioStream = allPositionsFileStream
    .Distinct($"{TaskName}: Distinct portfolios", i => GetKbcClientCode(i.ClientNbr))
    .Select($"{TaskName}: Create portfolios", euroCurrencyStream, (l, r) => new DiscretionaryPortfolio
    {
        InternalCode = GetKbcClientCode(l.ClientNbr),
        Name = GetKbcClientCode(l.ClientNbr),
        ShortName = GetKbcClientCode(l.ClientNbr),
        CurrencyId = r.Id,
        InceptionDate = DateTime.Today,
        PricingFrequency = FrequencyType.Daily
    })    
    .EfCoreSave($"{TaskName}: Insert portfolios", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var updatePortfolioNameStream = allPositionsFileStream
	.Where($"{TaskName} Filter cash pos", i=> i.FilePath.Contains("FBAL_CLASS"))
    .Distinct($"{TaskName}: update Portfolio Name - Distinct clients", i => i.ClientNbr)
	.LookupPortfolio($"{TaskName} Get related portfolio to update", i => GetKbcClientCode(i.ClientNbr),
        (l,r) => new {FileRow = l, Portfolio = r})
    .Select($"{TaskName}: Update sub fund", i => {       
        i.Portfolio.Name = i.Portfolio.Name.ToLower().Contains("-kbc")? 
            CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.FileRow.ClientName.ToLower()) : i.Portfolio.Name;
        return i.Portfolio;
    })
    .EfCoreSave($"{TaskName}: save portfolio names");

#region Client data
var peopleStream = posiallStream
    .Where($"{TaskName}: filter individual clients",  i=> !string.IsNullOrWhiteSpace(i.ClientType) || i.ClientType == "P")
    .Distinct($"{TaskName}: Distinct person", i => GetKbcClientCode(i.ClientNbr))
    .Select($"{TaskName}: create person entity", euroCurrencyStream, (l,r) => new Person
    {
        InternalCode = GetKbcClientCode(l.ClientNbr),
        FirstName = "",
        LastName = GetKbcClientCode(l.ClientNbr),
        CurrencyId = r.Id,
        Culture = new CultureInfo("en"),
    })
    .EfCoreSave($"{TaskName}: save person", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var companyStream = allPositionsFileStream
    .WaitWhenDone($"{TaskName}: wait peopleStream", peopleStream)
    .Where($"{TaskName}: filter individual client", i=> string.IsNullOrWhiteSpace(i.ClientType) || i.ClientType != "P")
    .Distinct($"{TaskName}: Distinct companies", i => GetKbcClientCode(i.ClientNbr))
    .Select($"{TaskName}: create company entity", euroCurrencyStream, (l,r) => new Company
    {
        InternalCode = GetKbcClientCode(l.ClientNbr),
        Name = GetKbcClientCode(l.ClientNbr),
        CurrencyId = r.Id,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31)
    })
    .EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var updatePeopleNamesStream = fbalStream
    .Distinct($"{TaskName}: update people Name - Distinct clients", i => i.ClientNbr)
    .Lookup($"{TaskName} Get related person to update", peopleStream,
        i => GetKbcClientCode(i.ClientNbr), i => i.InternalCode, (l,r) => new {FileRow = l, Person = r})
    .Where($"{TaskName} filter not null person", i => i.Person != null)
    .Select($"{TaskName}: Update person entity", i => {
        i.Person.FirstName = string.IsNullOrWhiteSpace(i.Person.FirstName)? 
                CultureInfo.InvariantCulture.TextInfo.ToTitleCase(
                    ((i.FileRow.ClientName.Split(" ").Count() >1)? i.FileRow.ClientName.Split(" ").Last() : "" ).ToLower())
                : i.Person.FirstName;
        i.Person.LastName = i.Person.LastName.ToLower().Contains("-kbc")? 
                CultureInfo.InvariantCulture.TextInfo.ToTitleCase(((i.FileRow.ClientName.Split(" ").Count() > 1)? 
                    string.Join(" ", i.FileRow.ClientName.Split(" ").Take(i.FileRow.ClientName.Split(" ").Count()-1).ToArray()) 
                    : i.FileRow.ClientName).ToLower()) 
                :i.Person.LastName;
        return i.Person;
    })
    .EfCoreSave($"{TaskName}: save updated people names");

var updateCompanyNamesStream = fbalStream
    .Where($"{TaskName} Filter cash pos company", i => i.ClientType != "P")
    .Distinct($"{TaskName}: update company Name - Distinct clients", i => i.ClientNbr)
    .Lookup($"{TaskName} Get related people to name update 2",peopleStream,
        i => GetKbcClientCode(i.ClientNbr), i => i.InternalCode, (l,r) => new {FileRow = l, People = r})
	.Lookup($"{TaskName} Get related company to name update",companyStream,
        i => GetKbcClientCode(i.FileRow.ClientNbr), i => i.InternalCode, (l,r) => new {l.FileRow, l.People , Company = r})
    .Where($"{TaskName} filter not null Company", i => i.Company != null && i.People == null )    
    .Select($"{TaskName}: Update company entity", i => {
        i.Company.Name = i.Company.Name.ToLower().Contains("-kbc")? 
            CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.FileRow.ClientName.ToLower()) : i.Company.Name;
        return i.Company;
    })
    .EfCoreSave($"{TaskName}: save updated company names");

var individualInvestorRelationshipStream = peopleStream
    .Select($"{TaskName} create individual Investors Relationship", i => 
        new {
            EntityInternalCode=i.InternalCode, 
            Relationship= new InvestorRelationship{
                EntityId = i.Id,
                InvestorType = InvestorType.Retail,
                StatementFrequency = FrequencyType.Quarterly,
                StartDate = DateTime.Today,
                CurrencyId = i.CurrencyId.Value,
            }
        })
    .EfCoreSave($"{TaskName}: Save Individual Investors Relationship", o => o
        .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));

var institutionalInvestorRelationshipStream = companyStream
    .Select($"{TaskName} create Institutional Investors Relationships", i => 
       new {
            EntityInternalCode=i.InternalCode, 
            Relationship= new InvestorRelationship{
                EntityId = i.Id,
                InvestorType = InvestorType.Retail,
                StatementFrequency = FrequencyType.Quarterly,
                StartDate = DateTime.Today,
                CurrencyId = i.CurrencyId.Value,
            }
        })
    .EfCoreSave($"{TaskName}: save Institutional Investors Relationships", o => o
        .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));

var investorsStream = individualInvestorRelationshipStream
    .Union($"{TaskName}: merge of the investor relationship streams", institutionalInvestorRelationshipStream);

var relationshipPortfoliosStream = investorsStream
	.Lookup($"{TaskName}: Link Investor-Porfolio - get related portfolio",portfolioStream, 
                i=>i.EntityInternalCode,i=>i.InternalCode, (l,r) => new {InvestorRelationship = l.Relationship, Portfolio = r})
	//.Where($"{TaskName}: Link Investor-Porfolio - Filter existing portfolio", i=>i.Portfolio !=null)
    .Select($"{TaskName}: create link between investor and related portfolio", i => new RelationshipPortfolio {
		RelationshipId = i.InvestorRelationship.Id, 
        PortfolioId = i.Portfolio.Id
        })
	.Distinct($"{TaskName}: Distinct Relationship-Portfolio", i => new {i.RelationshipId, i.PortfolioId})
    .EfCoreSave($"{TaskName}: Save link Relationship-Portfolio", 
        o => o.SeekOn(i => new {i.RelationshipId, i.PortfolioId}).DoNotUpdateIfExists());
#endregion Client data

#region target Issuers, target subfund and target sicav
var issuingSicavsStream = posiallStream
    .Where($"{TaskName}: where is Share Classes for SICAV", i=>IsShareClassInstrType(i.SecType))
    .Distinct($"{TaskName}: distinct SecBase SICAV", i => i.SecHolder)
    .Select($"{TaskName}: Create Issuer SICAV",euroCurrencyStream, (i,j) => new Sicav{
        InternalCode = i.SecHolder + "-KBC",
        Name = GetSicavName(i.SecName),
        //CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = j.Id,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: save target issuer Sicavs", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//Set Sicav issuer reference to itself 
var issuingSicavsStreamFixIssuer = issuingSicavsStream
    .Fix($"{TaskName}: IssuerId ", i => i.FixProperty(i => i.IssuerId).AlwaysWith(i => i.Id))
    .EfCoreSave($"{TaskName}: Fixing Sicav issuer Id");

var issuingCompaniesStream = posiallStream
    .Distinct($"{TaskName}: distinct on SecHolder", i => i.SecHolder)
    .Select($"{TaskName}: Create Issuer companies",euroCurrencyStream,(i,j)=> new Company{
        InternalCode = i.SecHolder + "-KBC",
        Name = i.SecName,
        CurrencyId = j.Id,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSubFundsStream = posiallStream
    .Where($"{TaskName}: where is Share Classes for target sub-fund", i=>IsShareClassInstrType(i.SecType))
    .Distinct($"{TaskName}: distinct SecBase Sub-Funds", i => i.SecHolder)
    .Lookup($"{TaskName}: get related sub-fund Sicav", issuingSicavsStream, 
        i => i.SecHolder+"-KBC", i => i.InternalCode,
        (l,r) => new {FileRow = l, Sicav = r })
    .Select($"{TaskName}: Create target subFund", i => new SubFund{
        InternalCode = i.FileRow.SecHolder + "-KBC",
        Name =  GetSubFundName(i.FileRow.SecName),
        ShortName  =  "From KBC",
        SicavId = i.Sicav.Id,
        PricingFrequency = FrequencyType.Daily,
    })
    .EfCoreSave($"{TaskName}: save target sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

#endregion

string GetPositionCurrency(string filePath, string secCur, string cur)
    => filePath.Contains("FBAL_CLASS")? cur:secCur;

//2. CREATE TARGET SECURITIES
var targetSecurityStream = allPositionsFileStream
    .Distinct($"{TaskName}: distinct position securities", i =>  GetSecurityKbcCode(i.Isin, i.SecNbr, i.FilePath, i.ClientNbr, i.AccountNbr, i.Cur))
    .LookupCurrency($"{TaskName}: get related currency", l => GetPositionCurrency(l.FilePath, l.SecCur, l.Cur), 
        (l, r) => new { FileRow = l, Currency = r })
    .Lookup($"{TaskName} Get related issuing company",issuingCompaniesStream,
        i=>i.FileRow.SecHolder+"-KBC", i=>i.InternalCode,
        (l, r) => new { l.FileRow, l.Currency, IssuingCompany = r })
    .Lookup($"{TaskName} Get parent sub-fund",targetSubFundsStream,
        i=>i.FileRow.SecHolder+"-KBC", i=>i.InternalCode,
        (l, r) => new { l.FileRow, l.Currency, l.IssuingCompany, PargetSubFund = r })
    .Select($"{TaskName}: create target security", i => 
        CreateSecurity(i.FileRow.FilePath, i.FileRow.SecNbr, i.FileRow.SecType, i.FileRow.SecName, 
            i.Currency != null? i.Currency.Id : throw new Exception("Currency not provided for "+i.FileRow.SecNbr), 
            i.FileRow.Isin,i.FileRow.ClientNbr, i.FileRow.AccountNbr, i.FileRow.Cur, i.FileRow.Description,
            i.IssuingCompany, i.PargetSubFund))
    .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var portfolioCompositionStream = allPositionsFileStream
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => 
            new PortfolioComposition { Date = l.GenDate, PortfolioId = r.Id })
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioId, i.Date })
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = allPositionsFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, 
        (l, r) => new { FileRow = l.FileRow, Security = l.Security, Composition = r })
    .Aggregate($"{TaskName}: Sum positions duplicates within a file",
        i => new
        {
            SecurityId = i.Security != null? i.Security.Id : 
                throw new Exception($"Create position: Security not found: {i.FileRow.ClientName} - {i.FileRow.SecName}" ),
            CompositionId= i.Composition !=null? i.Composition.Id  : 
                throw new Exception($"Create position: composition not found: {i.FileRow.ClientName} - {i.FileRow.GenDate}"),
        },
        i => new
        {
            MarketValueInPortfolioCcy = (double) 0,
            MarketValueInSecurityCcy = (double?) 0,   
            Quantity = (double) 0,
            CostPrice = (double?) 0,
            Count = 0,
        },
        (a, v) => new
        {
            MarketValueInPortfolioCcy = a.MarketValueInPortfolioCcy + 
                                        ( (v.FileRow.FilePath.Contains("FBAL_CLASS"))? v.FileRow.BalanceXxx
                                        : (v.FileRow.FilePath.Contains("POSI_ALL"))  ? v.FileRow.ValPortIntInc
                                        : throw new Exception("unregonized file path: " + v.FileRow.FilePath)),
            MarketValueInSecurityCcy = a.Count==0? 
                                        a.MarketValueInSecurityCcy + (v.FileRow.FilePath.Contains("FBAL_CLASS")? 
                                        v.FileRow.Balance: v.FileRow.ValueSecIntIn)
                                        : (double ?) null,
            Quantity = v.FileRow.FilePath.Contains("FBAL_CLASS")? 1: a.Quantity + v.FileRow.Position,
            CostPrice = (a.Count==0 && v.FileRow.FilePath.Contains("POSI_ALL"))? v.FileRow.Price : (double ?) null,   
            Count = a.Count + 1,
        })
    .Select($"{TaskName}: create position", i => new Position
    {
        SecurityId = i.Key.SecurityId,
        PortfolioCompositionId = i.Key.CompositionId,
        MarketValueInPortfolioCcy = i.Aggregation.MarketValueInPortfolioCcy,
        MarketValueInSecurityCcy = i.Aggregation.MarketValueInSecurityCcy,
        Value = i.Aggregation.Quantity,
        CostPrice = i.Aggregation.CostPrice,
        BookCostInSecurityCcy = i.Aggregation.CostPrice != null? (i.Aggregation.CostPrice * i.Aggregation.Quantity) : (double ?) null,         
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));


return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream,
    peopleStream, companyStream, portfolioStream, institutionalInvestorRelationshipStream,
    investorsStream, relationshipPortfoliosStream, updatePeopleNamesStream,updateCompanyNamesStream);

#region Helpers

string GetSecurityKbcCode(string isin, string secNbr, string filePath, string clientNbr, string accountNbr, string cur)
    => filePath.Contains("FBAL_CLASS")? $"{clientNbr}-{accountNbr}-{cur}-KBC"
    : filePath.Contains("POSI_ALL")? !string.IsNullOrWhiteSpace(isin)? isin : secNbr+"-KBC"
    : throw new Exception("Filepath not recognized: "+ filePath);

string GetSicavName(string secName)
{
    string sicavName = secName.Split("(").First();

    if (secName.Contains("-") && secName.Split("-").First().Length > 2 )
        sicavName =  secName.Split("-").First().Replace("."," ");
    else if (secName.Contains(".") && secName.Split(".").First().Length > 2)
        sicavName =  secName.Split(".").First();
    
    return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(sicavName.ToLower());
}
    
string GetSubFundName(string secName)
{
    string subFundName=secName;
    if (secName.Contains("("))
        subFundName = string.Join(" ", secName.Split("(").Take(secName.Split("(").Count()-1).ToArray()).Replace("."," ");
    if (secName.Contains(".") && secName.Contains("-"))
        subFundName =  string.Join(" ", secName.Split("-").Take(secName.Split("-").Count()-1).ToArray()).Replace("."," ");
    else if (secName.Contains("."))
        subFundName = string.Join(" ", secName.Split(".").Take(secName.Split(".").Count()-1).ToArray()).Replace("."," ");
    else if (secName.Contains("-"))
        subFundName =  string.Join(" ", secName.Split("-").Take(secName.Split("-").Count()-1).ToArray()).Replace("."," ");
    
    return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(subFundName.ToLower());
}
bool IsShareClassInstrType(string SecType)
    => SecType.ToLower().Contains("fund");

Security CreateSecurity(string filePath, string secNbr,string secType, string secName, int currencyId, string isin, 
                        string clientNbr, string accountNbr, string cur,string description,
                        Company issuingCompany, SubFund parentSubFund)
{
    Security security = null;
    if (filePath.Contains("FBAL_CLASS"))
    {
        security = new Cash();
        security.InternalCode = GetSecurityKbcCode(null, null, filePath, clientNbr, accountNbr, cur);
        security.Name = description + "-" + cur;
        security.ShortName = clientNbr + "-" + cur ;
    }
    else if (filePath.Contains("POSI_ALL"))
    {
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
            default:
                throw new Exception("KBC POSI_ALL - unreognized secType: " + secType);
        }
        security.InternalCode =  GetSecurityKbcCode(isin, secNbr, filePath, null, null, null); 
        security.Name = secName;
        security.ShortName = secName.Truncate(MaxLengths.ShortName);
        if (security is SecurityInstrument securityInstrument)
            securityInstrument.Isin = isin;
        if (security is ShareClass shareClass)
        {
            if (parentSubFund == null)
                throw new Exception("Share class parent sub-fund not provided for " + secName);
            shareClass.SubFundId = parentSubFund.Id;
        }
        if (security is RegularSecurity regularSecurity)
        {
            if (regularSecurity == null)
                throw new Exception("Issuing company not provided for " + secName);
            regularSecurity.IssuerId = issuingCompany.Id;
            regularSecurity.PricingFrequency = FrequencyType.Daily;
        }
    }
    else
        throw new Exception("Unregonized file path: " + filePath);
    
    security.CurrencyId = currencyId;
    return security;
}
#endregion