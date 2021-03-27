var positionFileStream = FlatFileDefinition.Create(i => new
{
    Isin = i.ToColumn<string>("Isin"),
    Company = i.ToColumn<string>("Company"),
    Quantity = i.ToNumberColumn<double>("Quantity",","),
    ValuationDate =  i.ToDateColumn("Valuation Date", "dd.MM.yyyy"),
    Price = i.ToNumberColumn<double?>("Price",","),
    Currency = i.ToColumn<string>("Currency"),
    MarketValueLoc = i.ToNumberColumn<double?>("Market Value (loc)",","),
    FxRate = i.ToNumberColumn<double?>("FX Rate",","),
    PriceRef = i.ToNumberColumn<double?>("Price (Ref.)",","),
    CurrencyFund = i.ToColumn<string>("Currency Fund"),
    MarketValueRef = i.ToNumberColumn<double?>("Market Value (Ref.)",","),
    PercentageofNAV = i.ToNumberColumn<double?>("% of NAV",","),
    AssetLevelI = i.ToColumn<string>("Asset Level I"),
    AssetLevelII = i.ToColumn<string>("Asset Level II"),
    AccruedInterest = i.ToNumberColumn<double?>("Accrued Interest",","),
    AccruedInterestLocal = i.ToNumberColumn<double?>("Accrued Interest (local)",","),
    CompanyID = i.ToColumn<string>("Company-ID"),
    Country = i.ToColumn<string>("Country"),
    CountryOfCompany = i.ToColumn<string>("Country of company"),
    CountryOfRisk = i.ToColumn<string>("Country of risk"),
    Fund = i.ToColumn<string>("Fund"),
    FundNumber = i.ToColumn<string>("Fund number"),
    EquitySector = i.ToColumn<string>("Equity Sector"),
    EquitySubSector = i.ToColumn<string>("Equity Subsector"),
    DetailledInstrumentType = i.ToColumn<string>("Detailled instrument type"),
    InstrumentType1 = i.ToColumn<string>("Instrument type"),
    InstrumentType2 = i.ToColumn<string>("Instrument_type"),
    ID = i.ToColumn<string>("ID"),
    Issuer = i.ToColumn<string>("Issuer"),
    IssuerID = i.ToColumn<string>("Issuer-ID"),
    NAV = i.ToNumberColumn<double?>("NAV",","),
    PL = i.ToNumberColumn<double?>("P&L",","),
    PL_FX = i.ToNumberColumn<double?>("P&L FX",","),
    MarketValueDirty = i.ToNumberColumn<double?>("Market Value (dirty)",","),
    Security = i.ToColumn<string>("Security"),
    StoxxSector = i.ToColumn<string>("STOXX Sector"),
    StockExchange = i.ToColumn<string>("Stock exchange"),
    WKN = i.ToColumn<string>("WKN"),
    FilePath = i.ToSourceName(),
}).IsColumnSeparated(';').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse positions file", positionFileStream)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//Create SICAV
var sicavsStream = posFileStream
    .Distinct($"{TaskName} Distinct sicavsStream", i => i.Fund.Split(" - ").First())
    .LookupCurrency($"{TaskName}: get related SICAV currency",i => i.CurrencyFund, 
        (l,r) => new {FileRow = l,  Currency = r })
    .Select($"{TaskName}: Create sicav ",ProcessContextStream, (i,ctx) => new Sicav{
        InternalCode = i.FileRow.Fund.Split(" - ").First(),
        Name = i.FileRow.Fund.Split(" - ").First(),
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,        
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
        IssuerId = ctx.TenantId
    })
    .EfCoreSave($"{TaskName}: save sicav", o => o
        .SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//Create SubFund
string getPortfolioInternalCode(string fundNumber)
    => fundNumber + "-UI";

var subFundsStream = posFileStream
    .Distinct($"{TaskName}: distinct subFundsStream", i => i.FundNumber)
    .LookupCurrency($"{TaskName}: get related currency for fund", l => l.CurrencyFund,
        (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: lookup related sicav", sicavsStream, 
        (l, r) => new { l.FileRow, l.Currency, Sicav = r })
    .Select($"{TaskName}: create fund", i => new SubFund
    {
        InternalCode = getPortfolioInternalCode(i.FileRow.FundNumber),
        SicavId = i.Sicav.Id,
        Name = i.FileRow.Fund,
        ShortName = getPortfolioInternalCode(i.FileRow.FundNumber),
        CurrencyId = i.Currency?.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .EfCoreSave($"{TaskName}: save sub fund", o => o
        .SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// Portfolio TNA - ! overwrite existing
var tnaStream = posFileStream
    .Distinct($"{TaskName}: distinct funds-data", i => new { i.FundNumber, i.ValuationDate }, true)
    .CorrelateToSingle($"{TaskName}: get related subfund",subFundsStream,
        (l,r) => new {FileRow = l, SubFund = r})
    .Select($"{TaskName}: create sub fund tna", i => new PortfolioHistoricalValue
    {
        PortfolioId = i.SubFund.Id,
        Date = i.FileRow.ValuationDate,
        Type = HistoricalValueType.TNA,
        Value = i.FileRow.NAV.Value
    })
    .EfCoreSave($"{TaskName}: save share class hv", o => o
        .SeekOn(i => new { i.Date, i.PortfolioId, i.Type }));

//Create CASH SECURITIES
var targetCashStream = posFileStream
    .Where($"{TaskName}: keep cash (non-security) only", 
        i => string.IsNullOrEmpty(i.InstrumentType1)) // or i.AssetLevelI != "Securities"
    .Distinct($"{TaskName}: distinct positions cash", i => i.ID)
    .LookupCurrency($"{TaskName}: get related currency for cash", l => l.Currency,
            (l, r) => new { FileRow = l, Currency = r })
    .Select($"{TaskName}: create cash", i => new Cash{
        Name = i.FileRow.InstrumentType2,
        InternalCode = i.FileRow.ID,
        CurrencyId = (i.Currency != null)? i.Currency.Id : throw new Exception($"Currency not found {i.FileRow.ID}: "+ i.FileRow.Currency),
        ShortName = i.FileRow.Security.Truncate(MaxLengths.ShortName)
    })
    .EfCoreSave($"{TaskName}: save target cash", o => o.SeekOn(i => i.InternalCode)
        .DoNotUpdateIfExists())
    .Select($"{TaskName}: cast cash into Security", i => i as Security);

//Create TARGET SECURITIES
var issuerCompaniesStream = posFileStream
    .Where($"{TaskName}: filter null issuer", i => !string.IsNullOrEmpty(i.Issuer))
    .Distinct($"{TaskName}: distinct issuers", i => i.Issuer)    
    .LookupCountry($"{TaskName}: get issuer related country", l => l.CountryOfCompany, 
        (l,r) => new {FileRow = l, Country=r })
    .LookupCurrency($"{TaskName}: get related company currency", i => i.FileRow.Currency, 
        (l,r) => new {FileRow = l.FileRow, Country= l.Country, Currency = r })
    .Select($"{TaskName}: Create Issuer companies",i=> new Company{
        InternalCode = i.FileRow.Issuer,
        Name = i.FileRow.Issuer,
        CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,        
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        Regulated = false,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSecurityInstrumentStream = posFileStream
    .Where($"{TaskName}: keep security only", i => !string.IsNullOrEmpty(i.InstrumentType1)) //i.AssetLevelI == "Securities"
    .Distinct($"{TaskName}: distinct positions security", i => i.ID)
    .LookupCountry($"{TaskName}: get related country", i => GetCountryIsoFromUiName(i.Country), 
        (l, r) => new { FileRow = l, Country = r })
    .LookupCurrency($"{TaskName}: get related currency", l => l.FileRow.Currency,
        (l, r) => new { l.FileRow, l.Country, Currency = r })
    .Lookup($"{TaskName}: get related issuer",issuerCompaniesStream, i => i.FileRow.Issuer, i => i.InternalCode,
        (l,r) => new {l.FileRow, l.Country, l.Currency, Issuer = r})
    .Select($"{TaskName}: create instrument", i => 
        CreateSecurity(i.FileRow.ID, !string.IsNullOrEmpty(i.FileRow.Company)? i.FileRow.Company: i.FileRow.Security
            , i.Issuer, i.FileRow.AssetLevelII, i.FileRow.Isin, i.Currency, i.Country))
    .EfCoreSave($"{TaskName}: save target instrument", o => o
        .SeekOn(i => i.Isin).AlternativelySeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    .Select($"{TaskName}: cast instrument into Security", i => i as Security);

// Create POSITIONS
var targetSecurityStream = targetCashStream
    .Union($"{TaskName}: merge cash and target securities", targetSecurityInstrumentStream);


var portfolioCompositionStream = posFileStream
    .Distinct($"{TaskName}: distinct composition", i => new { i.FundNumber, i.ValuationDate }, true)
    .CorrelateToSingle($"{TaskName}: get composition portfolio", subFundsStream,
        (l, r) => new PortfolioComposition { PortfolioId = r.Id, Date = l.ValuationDate })
    .EfCoreDelete($"{TaskName} delete portfolio stress test", o => o
        .Set<FundProcess.Pms.DataAccess.Schemas.RiskMgmt.PortfolioCompositionStressTestImpact>()
        .Where((i, j) => j.PortfolioComposition.PortfolioId == i.PortfolioId && j.PortfolioComposition.Date == i.Date))    

    .EfCoreDelete($"{TaskName} delete portfolio statistics", o => o
        .Set<PortfolioStatistics>().Where((i, j) => j.PortfolioId == i.PortfolioId && j.Date == i.Date))    
    .EfCoreDelete($"{TaskName} delete compos if existing", o => o
        .Set<PortfolioComposition>().Where((i, j) => j.PortfolioId == i.PortfolioId && j.Date == i.Date))
    .EfCoreSave($"{TaskName}: save compositions", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r) =>
                        new { l.FileRow, Security = l.Security, Composition = r })
    .Aggregate($"{TaskName}: sum positions duplicates within a file",
        i => new
        {
            Path = i.FileRow.FilePath,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id
        },
        i => new
        {  
            Quantity = (double) 0,
            MarketValueRef = (double?) 0,
            MarketValueLoc= (double?) 0,
            PL = (double?) 0,
            PL_FX =  (double?) 0,
            AccruedInterest = (double?) 0,
            AccruedInterestLocal = (double?) 0,
        },
        (cur, i) => new{
            Quantity = cur.Quantity + i.FileRow.Quantity,
            MarketValueRef = cur.MarketValueRef+ i.FileRow.MarketValueRef,
            MarketValueLoc = cur.MarketValueLoc + i.FileRow.MarketValueLoc,
            PL = cur.PL + i.FileRow.PL,
            PL_FX = cur.PL_FX + i.FileRow.PL_FX,
            AccruedInterest = cur.AccruedInterest + i.FileRow.AccruedInterest,
            AccruedInterestLocal = cur.AccruedInterestLocal + i.FileRow.AccruedInterestLocal,
        })
        .Select($"{TaskName}: create position", i => new Position
        {
            PortfolioCompositionId = i.Key.CompositionId,
            SecurityId = i.Key.SecurityId,
            MarketValueInPortfolioCcy = i.Aggregation.MarketValueRef.Value,
            MarketValueInSecurityCcy = i.Aggregation.MarketValueLoc.Value,
            Value = i.Aggregation.Quantity,
            // BookCostInPortfolioCcy = i.Values.BookCostInPortfolioCcy,
            // BookCostInSecurityCcy = i.Values.BookCostInSecurityCcy,
            ProfitLossOnFxPortfolioCcy = i.Aggregation.PL_FX.Value,
            ProfitLossOnMarketPortfolioCcy = i.Aggregation.PL.Value,
            // NbAccruedDays = i.NumberOfAccruedDays,
            AccruedInterestInPortfolioCcy = i.Aggregation.AccruedInterest.Value,
            AccruedInterestInSecurityCcy = i.Aggregation.AccruedInterestLocal.Value
        })
        .ComputeWeight(TaskName)
        .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));

//Create EQUITY SECTOR CLASSIFICATION
// 1. Type definition
var equitySectorType = ProcessContextStream
    .Select($"{TaskName}: Create Equity Sector classification type", ctx => new SecurityClassificationType { Code = "EquitySector", 
                Name = new MultiCultureString { ["en"] = "Equity Sector" },
                Description = new MultiCultureString { ["en"] = "Equity Sector from Universal Investment" } 
        })
    .EfCoreSave($"{TaskName}: Save Equity Sector type", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure Equity Sector type is single");

// 2.1 Classification definition: EquitySector
var equitySectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct Equity sector", i => i.EquitySector.Trim())
    .Select($"{TaskName}: Get related classification type", equitySectorType, (i, ct) => new Classification
    {
        Code = i.EquitySector.Trim(),
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EquitySector.Trim()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save Equity Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// 2.2 Sub Classification definition: EQUITY SUB SECTOR
var equitySubSectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct Equity subsector", i => i.EquitySubSector.Trim())
    .CorrelateToSingle($"{TaskName}: lookup parent equity sector class", equitySectorClassificationStream,
                        (l, r) => new { FileRow = l, ParentClass = r })
    .Select($"{TaskName}: Get related type", equitySectorType, (i, ct) => new Classification
    {
        ClassificationTypeId = ct.Id,
        Code = i.FileRow.EquitySubSector.Trim(),
        ParentId = i.ParentClass.Id,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.FileRow.EquitySubSector.Trim()) }
    })
    .EfCoreSave($"{TaskName}: Save Equity Sub-Sector ", o => o
        .SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// 3. Classification assignation
var equitySubSectorAssignations = targetSecurityInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related equity sub sector", equitySubSectorClassificationStream,
        (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });

//-------------------STOXX SECTOR CLASSIFICATION-------------------
// 1. Type definition
var stoxxSectorType = ProcessContextStream
    .Select($"{TaskName}: Create stoxx Sector classification type", ctx => new SecurityClassificationType
    {
        Code = "stoxxSector",
        Name = new MultiCultureString { ["en"] = "STOXX Sector" },
        Description = new MultiCultureString { ["en"] = "STOXX Sector from Universal Investment" }
    })
    .EfCoreSave($"{TaskName}: Save STOXX Sector type", o => o.SeekOn(ct => ct.Code)
    .DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure STOXX Sector type is single");

// 2. Classification definition: StoxxSector
var stoxxSectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct stoxx sector", i => i.StoxxSector.Trim())
    .Select($"{TaskName}: Get stoxx classification type", stoxxSectorType, (i, ct) => new Classification
    {
        Code = i.StoxxSector.Trim(),
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.StoxxSector.Trim()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save stoxx Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code })
    .DoNotUpdateIfExists());
// 3. Classification assignation
var stoxxSectorAssignations = targetSecurityInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related stoxx sector", stoxxSectorClassificationStream,
        (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });

//-------------------SAVE CLASSIFICATIONS-----------
var classificationOfSecurityStream = equitySubSectorAssignations
     .Union($"{TaskName}: union equity sector & stoxx sector assignations", stoxxSectorAssignations)
     .EfCoreSave($"{TaskName}: Insert classifications of security", o => o.SeekOn(i => new { i.SecurityId, i.ClassificationTypeId })
     .DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait until all positions/classifications of security are saved",
            positionStream, classificationOfSecurityStream,stoxxSectorAssignations,equitySubSectorAssignations
            ,tnaStream,targetSecurityInstrumentStream);

//----------HELPERS-----------------
string GetInternalCode(string isin,string securityCode)
    => !string.IsNullOrEmpty(isin)? isin : securityCode;

SecurityInstrument CreateSecurity(string securityCode, string secName, Company issuer, 
                        string secType, string isin, Currency currency, Country country)
{
    SecurityInstrument security = null;
    switch (secType)
    {
        case "Equities":
            security = new Equity();
            break;
        case "Bond":
            security = new Bond();
            break;
        default:
            throw new Exception("Not implemented: " + secType);
    }
    security.InternalCode = GetInternalCode(isin,securityCode);
    security.CurrencyId = (currency != null)? currency.Id : (int?) null;
    security.Name = secName;

    security.ShortName = security.Name.Truncate(MaxLengths.ShortName);

    if (security is SecurityInstrument securityInstrument)
        securityInstrument.Isin = isin;

    // if (security is Derivative der)
    //     der.MaturityDate = maturityDate;
    // if (security is StandardDerivative standardDerivative)
    //     standardDerivative.Nominal = nominal;
    // if (security is OptionFuture optFut)
    //     optFut.UnderlyingIsin = instrumentIsin;
    // if (security is Bond bond)
    // {
    //     bond.CouponFrequency = MapPeriodicity(couponPeriodicity);
    // }

    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.PricingFrequency = FrequencyType.Daily;
        regularSecurity.CountryId = (country != null)? country.Id : (int?) null;
        regularSecurity.IssuerId = (issuer != null)? issuer.Id : throw new Exception("Issuer not found for: " + secName); 
    }

    return security;
}

string GetCountryIsoFromUiName(string countryLabel)
{
    if (countryLabel == "Other")
        return string.Empty; 
    Dictionary<string, string> fromCountryNameToAlpha2Dictionary = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
    {
        ["Afghanistan"] = "AF",
        ["Albania"] = "AL",
        ["Algeria"] = "DZ",
        ["American Samoa"] = "AS",
        ["Andorra"] = "AD",
        ["Angola"] = "AO",
        ["Anguilla"] = "AI",
        ["Antarctica"] = "AQ",
        ["Antigua and Barbuda"] = "AG",
        ["Argentina"] = "AR",
        ["Armenia"] = "AM",
        ["Aruba"] = "AW",
        ["Ascension Island"] = "AC",
        ["Australia"] = "AU",
        ["Austria"] = "AT",
        ["Azerbaijan"] = "AZ",
        ["Bahamas"] = "BS",
        ["Bahrain"] = "BH",
        ["Bangladesh"] = "BD",
        ["Barbados"] = "BB",
        ["Belarus"] = "BY",
        ["Belgium"] = "BE",
        ["Belize"] = "BZ",
        ["Benin"] = "BJ",
        ["Bermuda"] = "BM",
        ["Bhutan"] = "BT",
        ["Bolivia"] = "BO",
        ["Bosnia and Herzegovina"] = "BA",
        ["Botswana"] = "BW",
        ["Bouvet Island"] = "BV",
        ["Brazil"] = "BR",
        ["British Indian Ocean Territory"] = "IO",
        ["Brunei Darussalam"] = "BN",
        ["Bulgaria"] = "BG",
        ["Burkina Faso"] = "BF",
        ["Burundi"] = "BI",
        ["Cabo Verde"] = "CV",
        ["Cambodia"] = "KH",
        ["Cameroon"] = "CM",
        ["Canada"] = "CA",
        ["Canary Islands"] = "IC",
        ["Cayman Islands"] = "KY",
        ["Central African Republic"] = "CF",
        ["Chad"] = "TD",
        ["Chile"] = "CL",
        ["China"] = "CN",
        ["Christmas Island"] = "CX",
        ["Clipperton Island"] = "CP",
        ["Colombia"] = "CO",
        ["Comoros"] = "KM",
        ["Congo"] = "CG",
        ["Cook Islands"] = "CK",
        ["Costa Rica"] = "CR",
        ["Cote d'Ivoire"] = "CI",
        ["Croatia"] = "HR",
        ["Cuba"] = "CU",
        ["Curaçao"] = "CW",
        ["Cyprus"] = "CY",
        ["Czechia"] = "CZ",
        ["Denmark"] = "DK",
        ["Diego Garcia"] = "DG",
        ["Djibouti"] = "DJ",
        ["Dominica"] = "DM",
        ["Dominican Republic"] = "DO",
        ["Ecuador"] = "EC",
        ["Egypt"] = "EG",
        ["El Salvador"] = "SV",
        ["Equatorial Guinea"] = "GQ",
        ["Eritrea"] = "ER",
        ["Estonia"] = "EE",
        ["Eswatini"] = "SZ",
        ["Ethiopia"] = "ET",
        ["European Union"] = "EU",
        ["Eurozone"] = "EZ",
        ["Falkland Islands"] = "FK",
        ["Faroe Islands"] = "FO",
        ["Fiji"] = "FJ",
        ["Finland"] = "FI",
        ["France"] = "FR",
        ["French Guiana"] = "GF",
        ["French Polynesia"] = "PF",
        ["Gabon"] = "GA",
        ["Gambia"] = "GM",
        ["Georgia"] = "GE",
        ["Germany"] = "DE",
        ["Ghana"] = "GH",
        ["Gibraltar"] = "GI",
        ["Greece"] = "GR",
        ["Greenland"] = "GL",
        ["Grenada"] = "GD",
        ["Guadeloupe"] = "GP",
        ["Guam"] = "GU",
        ["Guatemala"] = "GT",
        ["Guernsey"] = "GG",
        ["Guinea"] = "GN",
        ["Guinea-Bissau"] = "GW",
        ["Guyana"] = "GY",
        ["Haiti"] = "HT",
        ["Holy See"] = "VA",
        ["Honduras"] = "HN",
        ["Hong Kong"] = "HK",
        ["Hungary"] = "HU",
        ["Iceland"] = "IS",
        ["India"] = "IN",
        ["Indonesia"] = "ID",
        ["Iran"] = "IR",
        ["Iraq"] = "IQ",
        ["Ireland"] = "IE",
        ["Isle of Man"] = "IM",
        ["Israel"] = "IL",
        ["Italy"] = "IT",
        ["Jamaica"] = "JM",
        ["Japan"] = "JP",
        ["Jersey"] = "JE",
        ["Jordan"] = "JO",
        ["Kazakhstan"] = "KZ",
        ["Kenya"] = "KE",
        ["Kiribati"] = "KI",
        ["Kuwait"] = "KW",
        ["Kyrgyzstan"] = "KG",
        ["Latvia"] = "LV",
        ["Lebanon"] = "LB",
        ["Lesotho"] = "LS",
        ["Liberia"] = "LR",
        ["Libya"] = "LY",
        ["Liechtenstein"] = "LI",
        ["Lithuania"] = "LT",
        ["Luxembourg"] = "LU",
        ["Macao"] = "MO",
        ["Madagascar"] = "MG",
        ["Malawi"] = "MW",
        ["Malaysia"] = "MY",
        ["Maldives"] = "MV",
        ["Mali"] = "ML",
        ["Malta"] = "MT",
        ["Marshall Islands"] = "MH",
        ["Martinique"] = "MQ",
        ["Mauritania"] = "MR",
        ["Mauritius"] = "MU",
        ["Mayotte"] = "YT",
        ["Mexico"] = "MX",
        ["Moldova"] = "MD",
        ["Monaco"] = "MC",
        ["Mongolia"] = "MN",
        ["Montenegro"] = "ME",
        ["Montserrat"] = "MS",
        ["Morocco"] = "MA",
        ["Mozambique"] = "MZ",
        ["Myanmar"] = "MM",
        ["Namibia"] = "NA",
        ["Nauru"] = "NR",
        ["Nepal"] = "NP",
        ["Netherlands"] = "NL",
        ["New Caledonia"] = "NC",
        ["New Zealand"] = "NZ",
        ["Nicaragua"] = "NI",
        ["Niger"] = "NE",
        ["Nigeria"] = "NG",
        ["Niue"] = "NU",
        ["Norfolk Island"] = "NF",
        ["North Macedonia"] = "MK",
        ["Northern Mariana Islands"] = "MP",
        ["Norway"] = "NO",
        ["Oman"] = "OM",
        ["Pakistan"] = "PK",
        ["Palau"] = "PW",
        ["Palestine"] = "PS",
        ["Panama"] = "PA",
        ["Papua New Guinea"] = "PG",
        ["Paraguay"] = "PY",
        ["Peru"] = "PE",
        ["Philippines"] = "PH",
        ["Pitcairn"] = "PN",
        ["Poland"] = "PL",
        ["Portugal"] = "PT",
        ["Puerto Rico"] = "PR",
        ["Qatar"] = "QA",
        ["Romania"] = "RO",
        ["Russia"] = "RU",
        ["Rwanda"] = "RW",
        ["Saint Helena"] = "SH",
        ["Saint Lucia"] = "LC",
        ["Saint Martin"] = "MF",
        ["Saint Pierre and Miquelon"] = "PM",
        ["Saint Vincent and the Grenadines"] = "VC",
        ["Samoa"] = "WS",
        ["San Marino"] = "SM",
        ["Sao Tome and Principe"] = "ST",
        ["Saudi Arabia"] = "SA",
        ["Senegal"] = "SN",
        ["Serbia"] = "RS",
        ["Seychelles"] = "SC",
        ["Sierra Leone"] = "SL",
        ["Singapore"] = "SG",
        ["Slovakia"] = "SK",
        ["Slovenia"] = "SI",
        ["Solomon Islands"] = "SB",
        ["Somalia"] = "SO",
        ["South Africa"] = "ZA",
        ["South Sudan"] = "SS",
        ["South-Korea"] = "KR",
        ["Spain"] = "ES",
        ["Sri Lanka"] = "LK",
        ["Sudan"] = "SD",
        ["Suriname"] = "SR",
        ["Sweden"] = "SE",
        ["Switzerland"] = "CH",
        ["Syrian Arab Republic"] = "SY",
        ["Taiwan"] = "TW",
        ["Tajikistan"] = "TJ",
        ["Tanzania"] = "TZ",
        ["Thailand"] = "TH",
        ["Timor-Leste"] = "TL",
        ["Togo"] = "TG",
        ["Tokelau"] = "TK",
        ["Tonga"] = "TO",
        ["Trinidad and Tobago"] = "TT",
        ["Tunisia"] = "TN",
        ["Turkey"] = "TR",
        ["Turkmenistan"] = "TM",
        ["Turks and Caicos Islands"] = "TC",
        ["Tuvalu"] = "TV",
        ["UAE"] = "AE",
        ["Uganda"] = "UG",
        ["Ukraine"] = "UA",
        ["United Kingdom"] = "GB",
        ["United States"] = "US",
        ["USA"] = "US",
        ["Uruguay"] = "UY",
        ["Uzbekistan"] = "UZ",
        ["Vanuatu"] = "VU",
        ["Venezuela"] = "VE",
        ["Viet Nam"] = "VN",
        ["Virgin Islands"] = "VG",
        ["Western Sahara"] = "EH",
        ["Yemen"] = "YE",
        ["Zambia"] = "ZM",
        ["Zimbabwe"] = "ZW",
    };
    if (!fromCountryNameToAlpha2Dictionary.ContainsKey(countryLabel))
        throw new Exception("Please add the following country label to the country mapping list in the macro: " + countryLabel);
    return fromCountryNameToAlpha2Dictionary[countryLabel];
}