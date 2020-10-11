//"NumberId";"WKN";"Isin";"Security";"% Asset Class";"% of NAV";"Accrued Interest";"Accrued Interest (local)";"Asset Level I";"Asset Level II";"Bond Sector I";"Bond Sector II";"Book Price";"Book Value";"Book Value (Local)";"Contractsize";"Convexity";"Counterpart";"Country";"Coupon";"Credit pvbp";"Currency";"Currency Fund";"Delta";"Duration";"Effective Duration";"Equity Sector";"Equity Subsector";"Exposure";"Exposure Level I";"Exposure Level II";"Fund";"FX Rate";"FX Rate Acquisition";"Id";"Issuer";"Market Value (dirty)";"Market Value (loc)";"Market Value (Ref.)";"Maturity";"Maturity";"Mod. Duration";"NAV";"OAS";"Option Type";"Optionstyle";"P&L";"P&L FX";"Price";"Price (Ref.)";"PVBP";"Quantity";"Quantity (opt.)";"Rating";"Rating Grade";"Spread Duration";"STOXX Sector";"Underlying";"Valuation Date";"Warrant Opt.Type";"Yield";"Yield at Acquisition";"Call date";"Rank";"Inflation coefficient";"Interest type";"Coupon date";"Interest term";"Coupon month";"Pool factor";"Country of risk"
var positionFileStream = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("NumberId"), //Portfolio Code
    Fund = i.ToColumn<string>("Fund"), // Portfolio Name
    Isin = i.ToColumn<string>("Isin"),
    Security = i.ToColumn<string>("Security"),
    SecurityCode = i.ToColumn<string>("Id"), //Security Id

    Currency = i.ToColumn<string>("Currency"),
    CurrencyFund = i.ToColumn<string>("Currency Fund"),
    
    MarketValueDirty = i.ToColumn<double?>("Market Value (dirty)"),
    MarketValueLoc = i.ToColumn<double?>("Market Value (loc)"),
    MarketValueRef = i.ToColumn<double?>("Market Value (Ref.)"),
    Quantity = i.ToColumn<double?>("Quantity"),

    AssetLevelI = i.ToColumn<string>("Asset Level I"), //Securities, Cash, Claims / Liabilities
    AssetLevelII = i.ToColumn<string>("Asset Level II"), //Asset Type

    Country = i.ToColumn<string>("Country"),

    // BookPrice = i.ToColumn<double?>("Book Price"),
    // BookValue = i.ToColumn<double?>("Book Value"),
    // BookValueLocal = i.ToColumn<double?>("Book Value (Local)"),
    // Issuer = i.ToColumn<string>("Issuer"),
    // NAV = i.ToColumn<double?>("NAV"), //Portfolio TNA
    
    // AccruedInterest = i.ToColumn<string>("Accrued Interest"),
    // AccruedInterestLocal = i.ToColumn<string>("Accrued Interest (local)"),
    // Contractsize = i.ToColumn<string>("Contractsize"),
    // Rating = i.ToColumn<string>("Rating"),


    // //----------Classifications
    // RatingGrade = i.ToColumn<string>("Rating Grade"),
    // EquitySector = i.ToColumn<string>("Equity Sector"),
    // EquitySubsector = i.ToColumn<string>("Equity Subsector"),
    // BondSectorI = i.ToColumn<string>("Bond Sector I"),
    // BondSectorII = i.ToColumn<string>("Bond Sector II"),
    // Exposure = i.ToColumn<string>("Exposure"),
    // ExposureLevelI = i.ToColumn<string>("Exposure Level I"),
    // ExposureLevelII = i.ToColumn<string>("Exposure Level II"),
    // STOXXSector = i.ToColumn<string>("STOXX Sector"),
    // //----------

    // WKN = i.ToColumn<string>("WKN"),
    // PercentageAssetClass = i.ToColumn<string>("% Asset Class"),
    // PercentageofNAV = i.ToColumn<string>("% of NAV"),
    // CountryOfRisk = i.ToColumn<string>("Country of risk"),
    // Convexity = i.ToColumn<string>("Convexity"),
    // Counterpart = i.ToColumn<string>("Counterpart"),
    // Coupon = i.ToColumn<string>("Coupon"),
    // CreditPvbp = i.ToColumn<string>("Credit pvbp"),
    // Delta = i.ToColumn<string>("Delta"),
    // Duration = i.ToColumn<string>("Duration"),
    // EffectiveDuration = i.ToColumn<string>("Effective Duration"),
    // FXRate = i.ToColumn<string>("FX Rate"),
    // FXRateAcquisition = i.ToColumn<string>("FX Rate Acquisition"),
    // Maturity = i.ToColumn<string>("Maturity"),
    // Maturity = i.ToColumn<string>("Maturity"),
    // ModDuration = i.ToColumn<string>("Mod. Duration"),
    // OAS = i.ToColumn<string>("OAS"),
    // OptionType = i.ToColumn<string>("Option Type"),
    // Optionstyle = i.ToColumn<string>("Optionstyle"),
    // P & L = i.ToColumn<string>("P&L"),
    // P & LFX = i.ToColumn<string>("P&L FX"),
    // Price = i.ToColumn<string>("Price"),
    // PriceRef = i.ToColumn<string>("Price (Ref.)"),
    // PVBP = i.ToColumn<string>("PVBP"),
    // QuantityOpt = i.ToColumn<string>("Quantity (opt.)"),
    // SpreadDuration = i.ToColumn<string>("Spread Duration"),
    // Underlying = i.ToColumn<string>("Underlying"),
    // ValuationDate = i.ToColumn<string>("Valuation Date"),
    // WarrantOptType = i.ToColumn<string>("Warrant Opt.Type"),
    // Yield = i.ToColumn<string>("Yield"),
    // YieldAtAcquisition = i.ToColumn<string>("Yield at Acquisition"),
    // Calldate = i.ToColumn<string>("Call date"),
    // Rank = i.ToColumn<string>("Rank"),
    // InflationCoefficient = i.ToColumn<string>("Inflation coefficient"),
    // InterestType = i.ToColumn<string>("Interest type"),
    // CouponDate = i.ToColumn<string>("Coupon date"),
    // InterestTerm = i.ToColumn<string>("Interest term"),
    // CouponMonth = i.ToColumn<string>("Coupon month"),
    // PoolFactor = i.ToColumn<string>("Pool factor"),

}).IsColumnSeparated(';');

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse positions file", positionFileStream)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//Create SICAV
var sicavStream = posFileStream
    .Distinct($"{TaskName}: distinct sicavs", i => i.Fund)
    .Select($"{TaskName}: create sicav", ProcessContextStream, (i, ctx) => new Sicav
    {
        InternalCode = i.Fund,
        Name = i.Fund,
        IssuerId = ctx.TenantId
    })
    .EfCoreSave($"{TaskName}: save sicav", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//Create PORTFOLIO
var portfolioStream = posFileStream
    .Distinct($"{TaskName}: distinct funds", i => i.PortfolioCode, true)
    .LookupCurrency($"{TaskName}: get related currency for fund", l => l.CurrencyFund, (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: lookup related sicav", sicavStream, (l, r) => new { l.FileRow, l.Currency, Sicav = r })
    .Select($"{TaskName}: create fund", ProcessContextStream, (i, ctx) => new SubFund
    {
        InternalCode = i.FileRow.PortfolioCode,
        SicavId = i.Sicav.Id,
        Name = i.FileRow.PortfolioCode,
        ShortName = i.FileRow.PortfolioCode.Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .EfCoreSave($"{TaskName}: save sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//Create CASH lines
var targetCashStream = posFileStream
    .Where($"{TaskName}: keep cash (non-security) only", i => i.AssetLevelI != "Securities")
    .Distinct($"{TaskName}: distinct positions cash", i => i.SecurityCode)
    .LookupCountry($"{TaskName}: get related country for cash", l => l.Country, (l, r) => new { l.FileRow, l.SecurityCode, Country = r })
    .LookupCurrency($"{TaskName}: get related currency for cash", l => l.FileRow.Currency, (l, r) => new { l.FileRow, l.SecurityCode, l.Country, Currency = r })
    .Select($"{TaskName}: create cash", i => new Cash()
            InternalCode = i.SecurityCode,
            CurrencyId = i.Currency.Id,
            Name = i.FileRow.Name,
            ShortName = i.SecurityCode.Truncate(MaxLengths.ShortName)
    )
    .EfCoreSave($"{TaskName}: save target cash", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    //.Select($"{TaskName}: cast cash into Security", i => i as Security);

//Create TARGET SECURITIES lines
var targetInstrumentStream = posFileStream
    .Where($"{TaskName}: keep security only", i => i.AssetLevelI == "Securities")
    //.Fix($"{TaskName}: recompute isin", i => i.FixProperty(p => p.Isin).AlwaysWith(p => p.Isin == "-1" ? null : p.Isin))
    //.ReKey($"{TaskName}: Uniformize target instrument codes", i => new { i.Isin, i.InstrCode }, (i, k) => new { FileRow = i, Key = k })
    .Distinct($"{TaskName}: distinct positions security", i => i.SecurityCode)
    .LookupCountry($"{TaskName}: get related country", l => l.FileRow.Country, (l, r) => new { l.FileRow, l.SecurityCode, Country = r })
    .LookupCurrency($"{TaskName}: get related currency", l => l.FileRow.Currency, (l, r) => new { l.FileRow, l.SecurityCode, l.Country, Currency = r })
    .Select($"{TaskName}: create instrument", i => CreateSecurity(
                i.SecurityCode, i.FileRow.Security,i.FileRow.AssetLevelII, i.FileRow.Isin, i.Currency.Id,i.Country.Id, i.Contractsize,i.Rating) 
                as SecurityInstrument)
    .EfCoreSave($"{TaskName}: save target instrument", o => o.SeekOn(i => i.InternalCode).AlternativelySeekOn(i => i.Isin).DoNotUpdateIfExists())
    .Select($"{TaskName}: cast instrument into Security", i => i as Security);


// EQUITY SECTOR CLASSIFICATION
// 1. Type definition
var equitySectorType = ProcessContextStream
    .Select($"{TaskName}: Create Equity Sector classification type", ctx => new SecurityClassificationType { Code = "EquitySector", 
                                                                Name = new MultiCultureString { ["en"] = "Equity Sector" } })
    .EfCoreSave($"{TaskName}: Save Equity Sector type", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure Equity Sector type is single");

// 2.1 Classification definition: EquitySector
var equitySectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct Equity sector", i => i.EquitySector)
    .Select($"{TaskName}: Get related type", equitySectorType, (i, ct) => new SecurityClassification
    {
        Code = i.EquitySector,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EquitySector) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save Equity Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// 2.2 Classification definition: EquitySubSector
var equitySubSectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct Equity sector", i => i.EquitySector)
    .CorrelateToSingle($"{TaskName}: lookup parent equity sector class", equitySectorClassificationStream, (l, r) => new { l.FileRow, ParentClass = r })
    .Select($"{TaskName}: Get related type", equitySectorType, (i, ct) => new SecurityClassification
    {
        ClassificationTypeId = ct.Id,
        ParentId = ParentClass.Id,
        Code = i.EquitySubSector,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EquitySubSector) }
    })
    .EfCoreSave($"{TaskName}: Save Equity Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// 3. Classification assignation

var ecoSectorOfSecurityStream = targetInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related eco sector", ecoSectorClassificationStream, (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });


// STOXX SECTOR CLASSIFICATION
    // STOXXSector = i.ToColumn<string>("STOXX Sector"),

// BOND SECTOR CLASSIFICATION
    // BondSectorI = i.ToColumn<string>("Bond Sector I"),
    // BondSectorII = i.ToColumn<string>("Bond Sector II"),


// RATING GRADE CLASSIFICATION
    //RatingGrade = i.ToColumn<string>("Rating Grade"),
    

//==> string ratingGrade, string 



//----------HELPERS-----------------
Security CreateSecurity(string securityCode, string secName,string secType, string isin, int? currencyId, int? countryId, double? contractsize,
                        string rating)
{
    //if (string.IsNullOrWhiteSpace(secType) && !string.IsNullOrWhiteSpace(isin)) secType = "FUND";
    Security security = null;
    switch (secType)
    {
        case "Equities":
            security = new Equity();
            break;
        default:
            throw new Exception("Not implemented: "+secType);
    }

    if (security != null)
    {
        security.InternalCode = securityCode;
        security.CurrencyId = currencyId;
        // if (security is Derivative der)
        //     der.MaturityDate = maturityDate;
        // if (security is StandardDerivative standardDerivative)
        //     standardDerivative.Nominal = nominal;
        // if (security is OptionFuture optFut)
        //     optFut.UnderlyingIsin = instrumentIsin;
        //else if (security is SecurityInstrument securityInstrument)
            securityInstrument.Isin = instrumentIsin;
        // if (security is Bond bond)
        // {
        //     bond.CouponFrequency = MapPeriodicity(couponPeriodicity);
        // }
        security.Name = secName;
        security.ShortName = secName.Truncate(MaxLengths.ShortName);
    }
    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.CountryId = countryId;
    }

    return security;
}