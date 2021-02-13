//1. FILE MAPPING
var fileDefinition = FlatFileDefinition.Create(i => new
{
    BloombergTicker = i.ToColumn<string>("Bloomberg Ticker"),
    Categorie_Valeur = i.ToColumn<string>("Categorie_Valeur"),
    Clef_Alpha_Gest = i.ToColumn<string>("Clef_Alpha_Gest"),
    Date_Cours = i.ToOptionalDateColumn("Date_Cours", "dd/MM/yyyy"),
    Date_Position = i.ToOptionalDateColumn("Date_Position", "dd/MM/yyyy"),
    Date_Solde = i.ToOptionalDateColumn("Date_Solde", "dd/MM/yyyy"),
    Dernier_Cours = i.ToNumberColumn<double?>("Dernier_Cours","."),
    Devise = i.ToColumn<string>("Devise"),
    Intitule = i.ToColumn<string>("Intitule"),
    Intitule_Valeur = i.ToColumn<string>("Intitule_Valeur"),
    ISIN = i.ToColumn<string>("ISIN"),
    Matricule = i.ToColumn<string>("Matricule"),
    Nature = i.ToColumn<string>("Nature"),
    Nom_Gest = i.ToColumn<string>("Nom_Gest"),
    NOVALSYS = i.ToColumn<string>("NOVALSYS"),
    Numero = i.ToColumn<string>("Numero"),
    Place = i.ToColumn<string>("Place"),
    Qualite = i.ToColumn<string>("Qualite"),
    Quantite = i.ToNumberColumn<double?>("Quantite","."),
    Solde_Cash_Devise = i.ToNumberColumn<double?>("Solde_Cash_Devise","."),
    Solde_Cash_Eur = i.ToNumberColumn<double?>("Solde_Cash_Eur","."),
    Valorisation_Devise = i.ToNumberColumn<double?>("Valorisation_Devise","."),
    Valorisation_Eur= i.ToNumberColumn<double?>("Valorisation_Eur","."),
    Variante_Valeur= i.ToColumn<string>("Variante_Valeur"),
    FilePath = i.ToSourceName(),
}).IsColumnSeparated(';').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var allPositionsFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse position file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var secPosStream = allPositionsFileStream.Where($"{TaskName} where is positions",
        i => i.FilePath.ToLower().Contains("positions_titres"));

var cashBalancesStream = allPositionsFileStream.Where($"{TaskName} where is cash balances",
        i => i.FilePath.ToLower().Contains("soldes_cash"));

var euroCurrencyStream = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx,i) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

string GetDegroofClientCode(string matricule)
    => matricule + "-Degroof";

string GetDegroofPortfolioCode(string numero)
    => numero + "-Degroof";

var portfolioStream = allPositionsFileStream
    .Distinct($"{TaskName}: Distinct portfolios", i => GetDegroofPortfolioCode(i.Numero))
    .Select($"{TaskName}: Create portfolios", euroCurrencyStream, (l, r) => new { 
        Matricule = l.Matricule,
        DiscretionaryPortfolio = new DiscretionaryPortfolio
        {
            InternalCode = GetDegroofPortfolioCode(l.Numero),
            Name = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(l.Intitule.ToLower()),
            ShortName = l.Numero,
            CurrencyId = r.Id,
            InceptionDate = DateTime.Today,
            PricingFrequency = FrequencyType.Daily
        }
    })    
    .EfCoreSave($"{TaskName}: Insert portfolios", o => 
        o.Entity(i=>i.DiscretionaryPortfolio).SeekOn(i => i.InternalCode).DoNotUpdateIfExists()
        .Output((i,e)=> i));

#region Client data
var peopleStream = allPositionsFileStream
    .Where($"{TaskName}: filter individual clients",  i=> i.Qualite != "Org. Financement Pensions (002)")
    .Distinct($"{TaskName}: Distinct person", i => GetDegroofClientCode(i.Matricule))
    .Select($"{TaskName}: create person entity", euroCurrencyStream, (l,r) => new Person
    {
        InternalCode = l.Qualite != "Org. Financement Pensions (002)"? 
            throw new NotImplementedException("client types not implemented in people stream, please add"):
            GetDegroofClientCode(l.Matricule),
        FirstName = "",
        LastName = l.Intitule,
        CurrencyId = r.Id,
        Culture = new CultureInfo("en"),
    })
    .EfCoreSave($"{TaskName}: save person", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var clientCompanyStream = allPositionsFileStream
    .WaitWhenDone($"{TaskName}: wait peopleStream", peopleStream)
    .Where($"{TaskName}: where client is a company",  i=> i.Qualite == "Org. Financement Pensions (002)")
    .Distinct($"{TaskName}: Distinct companies", i => i.Matricule)
    .Select($"{TaskName}: create company entity", euroCurrencyStream, (l,r) => new Company
    {
        InternalCode = GetDegroofClientCode(l.Matricule),
        Name = l.Intitule,
        CurrencyId = r.Id,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31)
    })
    .EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

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

var institutionalInvestorRelationshipStream = clientCompanyStream
    .Select($"{TaskName} create Institutional Investors Relationships", i => 
       new {
            EntityInternalCode = i.InternalCode, 
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

var relationshipPortfoliosStream = portfolioStream
	.Lookup($"{TaskName}: Get related client", investorsStream, 
            i =>  GetDegroofClientCode(i.Matricule), i => i.EntityInternalCode, 
            (l,r) => new {
                Portfolio = l.DiscretionaryPortfolio,
                InvestorRelationship = r.Relationship 
            })
    .Select($"{TaskName}: create link between portfolio and its related client", i => new RelationshipPortfolio {
		RelationshipId = i.InvestorRelationship.Id, 
        PortfolioId = i.Portfolio.Id
        })
	.Distinct($"{TaskName}: Distinct Relationship-Portfolio", i => new {i.RelationshipId, i.PortfolioId})
    .EfCoreSave($"{TaskName}: Save link Relationship-Portfolio", 
        o => o.SeekOn(i => new {i.RelationshipId, i.PortfolioId}).DoNotUpdateIfExists());
#endregion Client data

#region target Issuers, target subfund and target sicav
var issuingSicavsStream = secPosStream
    .Where($"{TaskName}: where is Share Classes for SICAV", i => 
        !string.IsNullOrWhiteSpace(i.Categorie_Valeur)? IsShareClassInstrType(i.Categorie_Valeur)
        : throw new Exception($"Categorie_Valeur not provided for: {i.Intitule_Valeur} {i.Intitule}({i.FilePath})"))
    .Distinct($"{TaskName}: distinct SecBase SICAV", i => GetSicavName(i.Intitule_Valeur))
    .LookupCurrency($"{TaskName}: get related sicav ccy", i => i.Devise,
        (l,r) => new {FileRow = l, Currency = r})
    .Select($"{TaskName}: Create Issuer SICAV", i => new Sicav {
        InternalCode = GetSicavName(i.FileRow.Intitule_Valeur),
        Name = GetSicavName(i.FileRow.Intitule_Valeur),
        //CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = i.Currency.Id,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: save target issuer Sicavs", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// Set Sicav issuer reference to itself 
var issuingSicavsStreamFixIssuer = issuingSicavsStream
    .Fix($"{TaskName}: IssuerId ", i => i.FixProperty(i => i.IssuerId).AlwaysWith(i => i.Id))
    .EfCoreSave($"{TaskName}: Fixing Sicav issuer Id");

var issuingCompaniesStream = secPosStream
    .Distinct($"{TaskName}: distinct on SecHolder", i => i.NOVALSYS)
    .LookupCurrency($"{TaskName}: get related issuer ccy", i => i.Devise,
        (l,r) => new {FileRow = l, Currency = r})
    .Select($"{TaskName}: Create Issuer companies",(i,j)=> new Company{
        InternalCode = i.FileRow.NOVALSYS + "-Degroof",
        Name = i.FileRow.Intitule_Valeur,
        CurrencyId = i.Currency !=null? i.Currency.Id
                        : throw new Exception("Currency not recognized for: " + i.FileRow.Intitule_Valeur),
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSubFundsStream = secPosStream
    .Where($"{TaskName}: where is Share Classes for target sub-fund", i=> IsShareClassInstrType(i.Categorie_Valeur))
    .Distinct($"{TaskName}: distinct SecBase Sub-Funds", i => i.NOVALSYS)
    .Lookup($"{TaskName}: get related sub-fund Sicav", issuingSicavsStream, 
        i => GetSicavName(i.Intitule_Valeur) , i => i.InternalCode,
        (l,r) => new {FileRow = l, Sicav = r })
    .Select($"{TaskName}: Create target subFund", i => new SubFund{
        InternalCode = i.FileRow.NOVALSYS + "-Degroof",
        Name = GetSubFundName(i.FileRow.Intitule_Valeur),
        ShortName = "From Degroof",
        SicavId = i.Sicav.Id,
        PricingFrequency = FrequencyType.Daily,
    })
    .EfCoreSave($"{TaskName}: save target sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

#endregion

// string GetPositionCurrency(string filePath, string secCur, string cur)
//      => filePath.Contains("FBAL_CLASS")? cur:secCur;

// CREATE TARGET SECURITIES
var targetSecurityStream = allPositionsFileStream
    .Distinct($"{TaskName}: distinct position securities", 
            i => GetSecurityDegroofCode(i.FilePath, i.ISIN, i.NOVALSYS, i.Numero, i.Devise))
    .LookupCurrency($"{TaskName}: get related currency", i => i.Devise, 
        (l, r) => new { FileRow = l, Currency = r })
    .Lookup($"{TaskName} Get related issuing company",issuingCompaniesStream,
        i => i.FileRow.NOVALSYS + "-Degroof", i=>i.InternalCode,
        (l, r) => new { l.FileRow, l.Currency, IssuingCompany = r })
    .Lookup($"{TaskName} Get parent sub-fund", targetSubFundsStream,
        i => i.FileRow.NOVALSYS + "-Degroof", i=>i.InternalCode,
        (l, r) => new { l.FileRow, l.Currency, l.IssuingCompany, ParentSubFund = r })
    .Select($"{TaskName}: create target security", i => 
        CreateSecurity(i.FileRow.FilePath, i.FileRow.ISIN, i.FileRow.NOVALSYS, 
                        i.FileRow.Intitule_Valeur, i.FileRow.Numero,
                        i.FileRow.Categorie_Valeur, i.Currency, i.FileRow.Matricule,
                        i.IssuingCompany,i.ParentSubFund))
    .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

DateTime getPositionDate(DateTime? Date_Position, DateTime? Date_Solde)
    => Date_Position != null? Date_Position.Value
        : Date_Solde != null? Date_Solde.Value : throw new Exception("Position date not available");

var portfolioCompositionStream = allPositionsFileStream
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => 
            new PortfolioComposition { 
                Date = getPositionDate(l.Date_Position,l.Date_Solde), 
                PortfolioId = r.DiscretionaryPortfolio.Id 
            })
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioId, i.Date })
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionsStream = allPositionsFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, 
        (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, 
        (l, r) => new { FileRow = l.FileRow, Security = l.Security, Composition = r })
    .Aggregate($"{TaskName}: Sum positions duplicates within a file",
        i => new
        {
            SecurityId = i.Security != null? i.Security.Id : 
                throw new Exception($"Create position: Security not found: {i.FileRow.Intitule_Valeur}" ),
            CompositionId= i.Composition !=null? i.Composition.Id  : 
                throw new Exception($"Create position: composition not found: {i.FileRow.Intitule}"),
        },
        i => new
        {
            MarketValueInPortfolioCcy = (double) 0,
            MarketValueInSecurityCcy = (double?) 0,
            Quantity = (double) 0,
            CostPrice = (double?) null,
            Count = 0,
        },
        (a, v) => new
        {
            //MarketValueInPortfolioCcy = a.MarketValueInPortfolioCcy + 1,
            MarketValueInPortfolioCcy = a.MarketValueInPortfolioCcy + 
                        ( (v.FileRow.FilePath.ToLower().Contains("positions"))? v.FileRow.Valorisation_Eur.Value
                        : (v.FileRow.FilePath.ToLower().Contains("cash"))  ? v.FileRow.Solde_Cash_Eur.Value
                        : throw new Exception("unregonized file path: " + v.FileRow.FilePath)),
            MarketValueInSecurityCcy = a.Count==0? 
                ((v.FileRow.FilePath.ToLower().Contains("positions"))? v.FileRow.Valorisation_Devise.Value
                        : (v.FileRow.FilePath.ToLower().Contains("cash"))  ? v.FileRow.Solde_Cash_Devise.Value
                        : throw new Exception("unregonized file path: " + v.FileRow.FilePath))
                : (double ?) null,
            Quantity = v.FileRow.FilePath.ToLower().Contains("cash")? 1: a.Quantity + v.FileRow.Quantite.Value,
            CostPrice = v.FileRow.Dernier_Cours.HasValue? v.FileRow.Dernier_Cours.Value : a.CostPrice,
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
        // BookCostInSecurityCcy = i.Aggregation.CostPrice != null? 
        //             (i.Aggregation.CostPrice * i.Aggregation.Quantity) : (double ?) null,
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));

return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionsStream,
    peopleStream, clientCompanyStream, portfolioStream, institutionalInvestorRelationshipStream,
    investorsStream, relationshipPortfoliosStream);

#region Helpers

string GetSecurityDegroofCode(string filePath, string isin,string novalsys, string numero, string devise)
    => filePath.ToLower().Contains("cash")? $"{numero}-{devise}-Degroof"
    : filePath.ToLower().Contains("titres")? !string.IsNullOrWhiteSpace(isin)? isin : novalsys+"-Degroof"
    : throw new Exception("GetSecurityDegroofCode> File type not recognized: "+ filePath);

string GetSicavName(string secName)
{
     throw new NotImplementedException("GetSicavName in DEGROOF - Positions");
    // string sicavName = secName.Split("(").First();
    // if (secName.Contains("-") && secName.Split("-").First().Length > 2 )
    //     sicavName =  secName.Split("-").First().Replace("."," ");
    // else if (secName.Contains(".") && secName.Split(".").First().Length > 2)
    //     sicavName =  secName.Split(".").First();
    
    // return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(sicavName.ToLower());
}
    
string GetSubFundName(string secName)
{
    throw new NotImplementedException("GetSubFundName in DEGROOF - Positions");
    // string subFundName=secName;
    // if (secName.Contains("("))
    //     subFundName = string.Join(" ", secName.Split("(").Take(secName.Split("(").Count()-1).ToArray()).Replace("."," ");
    // if (secName.Contains(".") && secName.Contains("-"))
    //     subFundName =  string.Join(" ", secName.Split("-").Take(secName.Split("-").Count()-1).ToArray()).Replace("."," ");
    // else if (secName.Contains("."))
    //     subFundName = string.Join(" ", secName.Split(".").Take(secName.Split(".").Count()-1).ToArray()).Replace("."," ");
    // else if (secName.Contains("-"))
    //     subFundName =  string.Join(" ", secName.Split("-").Take(secName.Split("-").Count()-1).ToArray()).Replace("."," ");
    
    // return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(subFundName.ToLower());
}

bool IsShareClassInstrType(string SecType)
    => SecType.ToLower().Contains("fund");

Security CreateSecurity(string filePath, string isin, string novalsys, string intitule_Valeur, string numero,
                        string categorie_Valeur,  Currency currency, string matricule,
                         Company issuingCompany, SubFund parentSubFund)
{
    Security security = null;
    if (filePath.ToLower().Contains("cash"))
    {
        security = new Cash();
        security.InternalCode = GetSecurityDegroofCode(filePath, isin, novalsys, numero, currency.IsoCode);
        security.Name = GetSecurityDegroofCode(filePath, isin, novalsys, numero, currency.IsoCode);
        security.ShortName = matricule + "-" + currency.IsoCode ;
    }
    else if (filePath.ToLower().Contains("titres"))
    {
        switch (categorie_Valeur.ToLower())
        {
            case "certificat foncier":
            case "action":
                security = new Equity();
                break;
            default:
                throw new Exception("Degroof Positions Titres - unreognized security type, please add: " + categorie_Valeur);
        }
        security.InternalCode =  GetSecurityDegroofCode(filePath, isin, novalsys, numero, currency.IsoCode);
        security.Name = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(intitule_Valeur.ToLower());
        security.ShortName = intitule_Valeur.Truncate(MaxLengths.ShortName);
        if (security is SecurityInstrument securityInstrument)
            securityInstrument.Isin = isin;
        if (security is ShareClass shareClass)
        {
            if (parentSubFund == null)
                throw new Exception("Share class parent sub-fund not provided for " + intitule_Valeur);
            shareClass.SubFundId = parentSubFund.Id;
        }
        if (security is RegularSecurity regularSecurity)
        {
            if (regularSecurity == null)
                throw new Exception("Issuing company not provided for " + intitule_Valeur);
            regularSecurity.IssuerId = issuingCompany.Id;
            regularSecurity.PricingFrequency = FrequencyType.Daily;
        }
    }
    else
        throw new Exception("Unregonized file path: " + filePath);
    
    security.CurrencyId = currency != null? currency.Id
                : throw new Exception("Currency not provided for: " + intitule_Valeur);
    return security;
}
#endregion