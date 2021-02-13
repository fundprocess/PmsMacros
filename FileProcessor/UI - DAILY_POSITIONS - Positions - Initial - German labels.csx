var positionFileStream = FlatFileDefinition.Create(i => new
{
    Isin = i.ToColumn<string>("Isin"), //Isin
    //Company = i.ToColumn<string>("Wertpapier"), //Company
    Quantity = i.ToNumberColumn<double>("Nominale/Stücke",","), //Quantity
    ValuationDate =  i.ToDateColumn("Bewertungsdatum", "dd.MM.yyyy"), //Valuation Date
    Price = i.ToNumberColumn<double?>("Kurs",","),//Price
    Currency = i.ToColumn<string>("Währung"), //Currency
    MarketValueLoc = i.ToNumberColumn<double?>("Kurswert (lokal)",","), //Market Value (loc)
    FxRate = i.ToNumberColumn<double?>("Devisenkurs",","), //FX Rate
    PriceRef = i.ToNumberColumn<double?>("Kurs Fondwhr.",","), //Price (Ref.)
    CurrencyFund = i.ToColumn<string>("Currency Fund"), //Fondswährung
    MarketValueRef = i.ToNumberColumn<double?>("Kurswert Fondswhrg.",","), //Market Value (Ref.)
    //PercentageofNAV = i.ToNumberColumn<double?>("% of NAV",","),
    AssetLevelI = i.ToColumn<string>("Asset Level I"),
    AssetLevelII = i.ToColumn<string>("Asset Level II"),
    //AccruedInterest = i.ToNumberColumn<double?>("Accrued Interest",","),
    //AccruedInterestLocal = i.ToNumberColumn<double?>("Accrued Interest (local)",","),
    //CompanyID = i.ToColumn<string>("Company-ID"),
    //Country = i.ToColumn<string>("Country"),
    //CountryOfCompany = i.ToColumn<string>("Country of company"),
    //CountryOfRisk = i.ToColumn<string>("Country of risk"),
    Fund = i.ToColumn<string>("Fonds"), //Fund
    FundNumber = i.ToColumn<string>("Fondsnummer"), //Fund number
    EquitySector = i.ToColumn<string>("Aktien Sektor"), //Equity Sector
    EquitySubSector = i.ToColumn<string>("Aktien Subsektor"), //Equity Subsector
    //DetailledInstrumentType = i.ToColumn<string>("Detailled instrument type"),
    // InstrumentType1 = i.ToColumn<string>("Instrument type"),
    // InstrumentType2 = i.ToColumn<string>("Instrument_type"),
    ID = i.ToColumn<string>("Id"),
    Issuer = i.ToColumn<string>("Emittent"), //Issuer
    //IssuerID = i.ToColumn<string>("Issuer-ID"),
    //NAV = i.ToNumberColumn<double?>("NAV",","),
    PL = i.ToNumberColumn<double?>("GuV",","), //P&L
    PL_FX = i.ToNumberColumn<double?>("GuV FX",","), //P&L FX
    MarketValueDirty = i.ToNumberColumn<double?>("Kurswert (dirty)",","), //Market Value (dirty)
    Security = i.ToColumn<string>("Wertpapier"), //Security
    StoxxSector = i.ToColumn<string>("STOXX Sektor"), //STOXX Sector
    //StockExchange = i.ToColumn<string>("Stock exchange"),
    //WKN = i.ToColumn<string>("WKN"),
    FilePath = i.ToSourceName(),
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse positions file", positionFileStream)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//Create CASH lines
var targetCashStream = posFileStream
    .Where($"{TaskName}: keep cash (non-security) only", 
        i => i.AssetLevelI != "Wertpapiere" ) // or
    .Distinct($"{TaskName}: distinct positions cash", i => i.ID)
    .LookupCurrency($"{TaskName}: get related currency for cash", l => l.Currency,
            (l, r) => new { FileRow = l, Currency = r })
    .Select($"{TaskName}: create cash", i => new Cash{
        Name = i.FileRow.Security,
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
    .LookupCurrency($"{TaskName}: get related company currency", i => i.Currency, 
        (l,r) => new {FileRow = l, Currency = r })
    .Select($"{TaskName}: Create Issuer companies",i=> new Company{
        InternalCode = i.FileRow.Issuer,
        Name = i.FileRow.Issuer,
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,        
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        Regulated = false,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSecurityInstrumentStream = posFileStream
    .Where($"{TaskName}: keep security only", i => i.AssetLevelI == "Wertpapiere")
    .Distinct($"{TaskName}: distinct positions security", i => i.ID)
    .LookupCurrency($"{TaskName}: get related currency", l => l.Currency,
        (l, r) => new { FileRow = l,Currency = r })
    .Lookup($"{TaskName}: get related issuer",issuerCompaniesStream, i => i.FileRow.Issuer, i => i.InternalCode,
        (l,r) => new {l.FileRow, l.Currency, Issuer = r})
    .Select($"{TaskName}: create instrument", i => 
        CreateSecurity(i.FileRow.ID, !string.IsNullOrEmpty(i.FileRow.Issuer)? i.FileRow.Issuer: i.FileRow.Security
            ,i.Issuer, i.FileRow.AssetLevelII, i.FileRow.Isin, i.Currency))
    .EfCoreSave($"{TaskName}: save target instrument", o => o
        .SeekOn(i => i.Isin).AlternativelySeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    .Select($"{TaskName}: cast instrument into Security", i => i as Security);

// Create POSITIONS
var targetSecurityStream = targetCashStream
    .Union($"{TaskName}: merge cash and target securities", targetSecurityInstrumentStream);

//Create SubFund
string getPortfolioInternalCode(string fundNumber)
    => fundNumber + "-UI";

var portfolioCompositionStream = posFileStream
    .Distinct($"{TaskName}: distinct composition", i => new { i.FundNumber, i.ValuationDate }, true)
    .LookupPortfolio($"{TaskName} Get related Sub Fund",i=> getPortfolioInternalCode(i.FundNumber),
        (l,r) => new {FileRow = l , SubFund = r} )
    .Select($"{TaskName}: get composition portfolio", i => 
        new PortfolioComposition { 
            PortfolioId = (i.SubFund != null)? i.SubFund.Id : throw new Exception("SubFund not found:" + i.FileRow.FundNumber), 
            Date = i.FileRow.ValuationDate })
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }).DoNotUpdateIfExists());

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
            //AccruedInterest = (double?) 0,
            //AccruedInterestLocal = (double?) 0,
        },
        (cur, i) => new{
            Quantity = cur.Quantity + i.FileRow.Quantity,
            MarketValueRef = cur.MarketValueRef+ i.FileRow.MarketValueRef,
            MarketValueLoc = cur.MarketValueLoc + i.FileRow.MarketValueLoc,
            PL = cur.PL + i.FileRow.PL,
            PL_FX = cur.PL_FX + i.FileRow.PL_FX,
            //AccruedInterest = cur.AccruedInterest + i.FileRow.AccruedInterest,
            //AccruedInterestLocal = cur.AccruedInterestLocal + i.FileRow.AccruedInterestLocal,
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
            //AccruedInterestInPortfolioCcy = i.Aggregation.AccruedInterest.Value,
            //AccruedInterestInSecurityCcy = i.Aggregation.AccruedInterestLocal.Value
        })
        .ComputeWeight(TaskName)
        .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId })
                    .DoNotUpdateIfExists());

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
            ,positionStream,targetSecurityInstrumentStream);

//----------HELPERS-----------------
string GetInternalCode(string isin,string securityCode)
    => !string.IsNullOrEmpty(isin)? isin : securityCode;

SecurityInstrument CreateSecurity(string securityCode, string secName, Company issuer, 
                        string secType, string isin, Currency currency)
{
    SecurityInstrument security = null;
    switch (secType)
    {
        case "Aktien":
            security = new Equity();
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
        regularSecurity.IssuerId = (issuer != null)? issuer.Id : throw new Exception("Issuer not found for: " + secName); 
    }

    return security;
}