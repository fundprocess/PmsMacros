var positionFileStream = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("NumberId"), //Portfolio Code
    Fund = i.ToColumn<string>("Fund"), // Portfolio Name
    Isin = i.ToColumn<string>("Isin"),
    Security = i.ToColumn<string>("Security"),
    SecurityCode = i.ToColumn<string>("Id"), //Security Id
    ValuationDate = i.ToDateColumn("Valuation Date", "dd.MM.yyyy"),

    Currency = i.ToColumn<string>("Currency"),
    CurrencyFund = i.ToColumn<string>("Currency Fund"),
    Country = i.ToColumn<string>("Country"),

    MarketValueDirty = i.ToNumberColumn<double?>("Market Value (dirty)", ","),
    MarketValueLoc = i.ToNumberColumn<double?>("Market Value (loc)", ","),
    MarketValueRef = i.ToNumberColumn<double?>("Market Value (Ref.)", ","),
    Quantity = i.ToNumberColumn<double?>("Quantity", ","),

    AssetLevelI = i.ToColumn<string>("Asset Level I"), //Securities, Cash, Claims / Liabilities
    AssetLevelII = i.ToColumn<string>("Asset Level II"), //Asset Type

    BookPrice = i.ToNumberColumn<double?>("Book Price", ","),
    BookValue = i.ToNumberColumn<double?>("Book Value", ","),
    BookValueLocal = i.ToNumberColumn<double?>("Book Value (Local)", ","),

    AccruedInterest = i.ToNumberColumn<double?>("Accrued Interest", ","),
    AccruedInterestLocal = i.ToNumberColumn<double?>("Accrued Interest (local)", ","),
    ContractSize = i.ToNumberColumn<double?>("Contractsize", ","),
    Rating = i.ToColumn<string>("Rating"),

    Issuer = i.ToColumn<string>("Issuer"),
    TNA = i.ToNumberColumn<double?>("NAV", ","),

    FilePath = i.ToSourceName(),
    //----------Classifications
    EquitySector = i.ToColumn<string>("Equity Sector"),
    EquitySubSector = i.ToColumn<string>("Equity Subsector"),
    StoxxSector = i.ToColumn<string>("STOXX Sector"),
    RatingGrade = i.ToColumn<string>("Rating Grade"),
    // BondSectorI = i.ToColumn<string>("Bond Sector I"),
    // BondSectorII = i.ToColumn<string>("Bond Sector II"),
    // Exposure = i.ToColumn<string>("Exposure"),
    // ExposureLevelI = i.ToColumn<string>("Exposure Level I"),
    // ExposureLevelII = i.ToColumn<string>("Exposure Level II"),
    //----------

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
    .SetForCorrelation($"{TaskName}: Set correlation key")
    .Fix($"{TaskName}Fix some columns", o => o
         .FixProperty(i => i.PortfolioCode).IfNotNullWith(i => i.PortfolioCode.Substring(0, 4))
        )
    ;

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
    .Distinct($"{TaskName}: distinct funds", i => i.PortfolioCode, false)
    .LookupCurrency($"{TaskName}: get related currency for fund", l => l.CurrencyFund, (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: lookup related sicav", sicavStream, (l, r) => new { l.FileRow, l.Currency, Sicav = r })
    .Select($"{TaskName}: create fund", ProcessContextStream, (i, ctx) => new SubFund
    {
        InternalCode = i.FileRow.PortfolioCode,
        SicavId = i.Sicav.Id,
        Name = i.FileRow.Fund,
        ShortName = i.FileRow.Fund.Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .EfCoreSave($"{TaskName}: save sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

//Portfolio TNA
var managedSubFundStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get subfunds from db", (ctx, j) => ctx.Set<SubFund>());

var savedShareClassHvStream = posFileStream
    .Distinct($"{TaskName}: distinct funds for TNA", i => new { i.PortfolioCode, i.ValuationDate }, true)
    .Lookup($"{TaskName}: lookup related sub fund", managedSubFundStream, i => i.PortfolioCode, i => i.InternalCode, (l, r) => new { FileRow = l, SubFund = r })
    //.CorrelateToSingle($"{TaskName}: get hv related share class", managedSubFundStream, (l, r) => new { FromFile = l, FromDb = r })
    //.Where($"{TaskName}: Exclude empty hv", i=>i.FromFile.Value.HasValue)
    .Select($"{TaskName}: create Sub fund TNA hv", i => new PortfolioHistoricalValue
    {
        PortfolioId = i.SubFund.Id,
        Date = i.FileRow.ValuationDate,
        Type = HistoricalValueType.TNA,
        Value = i.FileRow.TNA.Value
    })
    .EfCoreSave($"{TaskName}: save share class hv", o => o.SeekOn(i => new { i.Date, i.PortfolioId, i.Type }));

//Create CASH lines
var targetCashStream = posFileStream
    .Where($"{TaskName}: keep cash (non-security) only", i => i.AssetLevelI != "Securities")
    .Distinct($"{TaskName}: distinct positions cash", i => i.SecurityCode)
    .LookupCountry($"{TaskName}: get related country for cash", l => l.Country, (l, r) => new { FileRow = l, l.SecurityCode, Country = r })
    .LookupCurrency($"{TaskName}: get related currency for cash", l => l.FileRow.Currency,
                                    (l, r) => new { FileRow = l.FileRow, SecurityCode = l.SecurityCode, l.Country, Currency = r })
    .Select($"{TaskName}: create cash", i => new Cash()
    {
        Name = i.FileRow.Security + "-ValuAnalysis",
        InternalCode = i.SecurityCode,
        CurrencyId = i.Currency?.Id,
        ShortName = i.SecurityCode.Truncate(MaxLengths.ShortName)
    }
    )
    .EfCoreSave($"{TaskName}: save target cash", o => o.SeekOn(i => i.InternalCode)
        .DoNotUpdateIfExists())
    .Select($"{TaskName}: cast cash into Security", i => i as Security);

//Create TARGET SECURITIES
var targetInstrumentStream = posFileStream
    .Where($"{TaskName}: keep security only", i => i.AssetLevelI == "Securities")
    //.ReKey($"{TaskName}: Uniformize target instrument codes", i => new { i.Isin, i.InstrCode }, (i, k) => new { FileRow = i, Key = k })
    .Distinct($"{TaskName}: distinct positions security", i => i.SecurityCode)
    .LookupCountry($"{TaskName}: get related country", l => GetCountryIsoFromUiName(l.Country), (l, r) => new { FileRow = l, l.SecurityCode, Country = r })
    .LookupCurrency($"{TaskName}: get related currency", l => l.FileRow.Currency,
                    (l, r) => new { l.FileRow, l.SecurityCode, l.Country, Currency = r })
    .Select($"{TaskName}: create instrument", i => CreateSecurity(
                i.SecurityCode, i.FileRow.Security, i.FileRow.Issuer, i.FileRow.AssetLevelII, i.FileRow.Isin, i.Currency?.Id, i.Country?.Id,
                i.FileRow.ContractSize, i.FileRow.Rating) as SecurityInstrument)
    .EfCoreSave($"{TaskName}: save target instrument", o => o.SeekOn(i => i.InternalCode).AlternativelySeekOn(i => i.Isin)
        .DoNotUpdateIfExists())
    .Select($"{TaskName}: cast instrument into Security", i => i as Security);

//Create POSITIONS
var targetSecurityStream = targetCashStream
    .Union($"{TaskName}: merge cash and target securities", targetInstrumentStream);

var portfolioCompositionStream = posFileStream
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.PortfolioCode, i.ValuationDate }, true)
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => new PortfolioComposition { Date = l.ValuationDate, PortfolioId = r.Id })
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r) =>
                        new { l.FileRow, Security = l.Security, Composition = r })
    .Aggregate($"{TaskName}: sum positions duplicates within a file",
        i => new
        {
            i.FileRow.FilePath,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id
        },
        i => new
        {
            i.FileRow,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id,
            Values = new
            {
                Quantity = (double?)0,
                MarketValueInPortfolioCcy = (double?)0,
                MarketValueInInstrCcy = (double?)0,
                BookCostInPortfolioCcy = (double?)0,
                BookCostInSecurityCcy = (double?)0,
                // // //UnreaResultOnFx = (double?)0,
                // // //UnreaOnStockExch = (double?)0,
                // // // NumberOfAccruedDays = (int?)0,
                AccruedInterestInPortfolioCcy = (double?)0,
                AccruedInterestInSecurityCcy = (double?)0,
                // Count = 0
            }
        },
        (a, v) =>
        new
        {
            v.FileRow,
            CompositionId = v.Composition.Id,
            SecurityId = v.Security.Id,
            Values = new
            {
                Quantity = a.Values.Quantity + v.FileRow.Quantity,
                MarketValueInPortfolioCcy = a.Values.MarketValueInPortfolioCcy + v.FileRow.MarketValueRef,
                MarketValueInInstrCcy = a.Values.MarketValueInInstrCcy + v.FileRow.MarketValueLoc,
                BookCostInPortfolioCcy = a.Values.BookCostInPortfolioCcy + v.FileRow.BookValue,
                BookCostInSecurityCcy = a.Values.BookCostInSecurityCcy + v.FileRow.BookValueLocal,
                //UnreaResultOnFx = a.Values.UnreaResultOnFx + v.FileRow.UnreaResultOnFx ?? 0,
                //UnreaOnStockExch = a.Values.UnreaOnStockExch + v.FileRow.UnreaOnStockExch ?? 0,
                // NumberOfAccruedDays = a.Values.NumberOfAccruedDays + v.FileRow.NumberOfAccruedDays ?? 0,
                AccruedInterestInPortfolioCcy = a.Values.AccruedInterestInPortfolioCcy + v.FileRow.AccruedInterest,
                AccruedInterestInSecurityCcy = a.Values.AccruedInterestInSecurityCcy + v.FileRow.AccruedInterestLocal,
                //Count = a.Values.Count + 1,
            }
        })
    .Select($"{TaskName}: get position aggregation",
        i =>
        new
        {
            CompositionId = i.Aggregation.CompositionId,
            SecurityId = i.Aggregation.SecurityId,
            // NumberOfAccruedDays = i.Aggregation.FileRow.NumberOfAccruedDays,
            Values = new
            {
                Quantity = i.Aggregation.Values.Quantity,// / i.Aggregation.Values.Count,
                i.Aggregation.Values.MarketValueInPortfolioCcy,
                i.Aggregation.Values.MarketValueInInstrCcy,
                i.Aggregation.Values.BookCostInPortfolioCcy,
                i.Aggregation.Values.BookCostInSecurityCcy,

                // i.Aggregation.Values.UnreaResultOnFx,
                // i.Aggregation.Values.UnreaOnStockExch,
                // NumberOfAccruedDays = (int)i.Aggregation.Values.NumberOfAccruedDays / i.Aggregation.Values.Count,
                i.Aggregation.Values.AccruedInterestInPortfolioCcy,
                i.Aggregation.Values.AccruedInterestInSecurityCcy,
            }
        }
        )
    .Distinct($"{TaskName}: exclude positions duplicates", i => new { i.CompositionId, i.SecurityId }, true)
    .Select($"{TaskName}: create position", i => new Position
    {
        PortfolioCompositionId = i.CompositionId,
        SecurityId = i.SecurityId,
        MarketValueInPortfolioCcy = i.Values.MarketValueInPortfolioCcy.Value,
        MarketValueInSecurityCcy = i.Values.MarketValueInInstrCcy,
        Value = i.Values.Quantity.Value,
        BookCostInPortfolioCcy = i.Values.BookCostInPortfolioCcy,
        BookCostInSecurityCcy = i.Values.BookCostInSecurityCcy,
        // //ProfitLossOnFxPortfolioCcy = i.Values.UnreaResultOnFx,
        // //ProfitLossOnMarketPortfolioCcy = i.Values.UnreaOnStockExch,
        // //NbAccruedDays = i.NumberOfAccruedDays,
        AccruedInterestInPortfolioCcy = i.Values.AccruedInterestInPortfolioCcy,
        AccruedInterestInSecurityCcy = i.Values.AccruedInterestInSecurityCcy
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
    .Distinct($"{TaskName}: Distinct Equity sector", i => i.EquitySector)
    .Select($"{TaskName}: Get related classification type", equitySectorType, (i, ct) => new SecurityClassification
    {
        Code = i.EquitySector,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EquitySector) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save Equity Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// 2.2 Sub Classification definition: EQUITY SUB SECTOR
var equitySubSectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct Equity subsector", i => i.EquitySubSector)
    .CorrelateToSingle($"{TaskName}: lookup parent equity sector class", equitySectorClassificationStream,
                        (l, r) => new { FileRow = l, ParentClass = r })
    .Select($"{TaskName}: Get related type", equitySectorType, (i, ct) => new SecurityClassification
    {
        ClassificationTypeId = ct.Id,
        Code = i.FileRow.EquitySubSector,
        ParentId = i.ParentClass.Id,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.FileRow.EquitySubSector) }
    })
    .EfCoreSave($"{TaskName}: Save Equity Sub-Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// 3. Classification assignation
var equitySubSectorAssignations = targetInstrumentStream
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
    .Distinct($"{TaskName}: Distinct stoxx sector", i => i.StoxxSector)
    .Select($"{TaskName}: Get stoxx classification type", stoxxSectorType, (i, ct) => new SecurityClassification
    {
        Code = i.StoxxSector,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.StoxxSector) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save stoxx Sector ", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code })
    .DoNotUpdateIfExists());
// 3. Classification assignation
var stoxxSectorAssignations = targetInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related stoxx sector", stoxxSectorClassificationStream,
        (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });


//-------------------RATING GRADE-------------------
// 1. Type definition
var ratingGradeType = ProcessContextStream
    .Select($"{TaskName}: Create rating grade classification type", ctx => new SecurityClassificationType
    {
        Code = "ratingGrade",
        Name = new MultiCultureString { ["en"] = "Rating Grade" },
        Description = new MultiCultureString { ["en"] = "rating grade from Universal Investment" }
    })
    .EfCoreSave($"{TaskName}: Save rating grade type", o => o.SeekOn(ct => ct.Code)
    .DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure rating grade type is single");

// 2. Classification definition: ratingGrade
var ratingGradeClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct rating grade", i => i.RatingGrade)
    .Select($"{TaskName}: Get rating grade classification type", ratingGradeType, (i, ct) => new SecurityClassification
    {
        Code = i.RatingGrade,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.RatingGrade) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save Rating Grade classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code })
    .DoNotUpdateIfExists());
// 3. Classification assignation
var ratingGradeAssignations = targetInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related rating grade", ratingGradeClassificationStream,
        (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });

//-------------------SAVE CLASSIFICATIONS-----------
var classificationOfSecurityStream = equitySubSectorAssignations
     .Union($"{TaskName}: union equity sector & stoxx sector assignations", stoxxSectorAssignations)
     .Union($"{TaskName}: union equity sector & stoxx sector & rating gade assignations", ratingGradeAssignations)
     .EfCoreSave($"{TaskName}: Insert classifications of security", o => o.SeekOn(i => new { i.SecurityId, i.ClassificationTypeId })
     .DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait until all positions/classifications of security are saved",
            positionStream, classificationOfSecurityStream);

//----------HELPERS-----------------

Security CreateSecurity(string securityCode, string secName, string issuer, string secType, string isin, int? currencyId, int? countryId
                        , double? contractsize, string rating)
{
    Security security = null;
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

    security.InternalCode = securityCode;
    security.CurrencyId = currencyId;
    security.Name = secName;

    if (security is Equity equity)
        security.Name = issuer;
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
        regularSecurity.CountryId = countryId;
    }
    if (security is StandardDerivative stdder)
        stdder.ContractSize = contractsize;

    return security;
}

string GetCountryIsoFromUiName(string countryLabel)
{
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
        ["Uruguay"] = "UY",
        ["USA"] = "US",
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