#region File Definition

var rbcPositionFileDefinition = FlatFileDefinition.Create(i => new
{
    FundCode = i.ToColumn<string>("FUND CODE"),
    FundName = i.ToColumn<string>("FUND NAME"),
    NavDate = i.ToDateColumn("NAV DATE", "yyyyMMdd"),
    MaturityDate = i.ToOptionalDateColumn("MATURITY DATE", "yyyyMMdd"),
    InternalNumber = i.ToColumn<string>("INTERNAL NUMBER"),
    IsinCode = i.ToColumn<string>("ISIN CODE"),
    InstrumentName = i.ToColumn<string>("INSTRUMENT NAME"),
    AccountNumber = i.ToColumn<string>("ACC NUMBER"),
    EconomicSectorLabel = i.ToColumn<string>("ECONOMIC SECTOR LABEL"),
    QuotationPlace = i.ToColumn<string>("QUOTATION PLACE"),
    SubFundCcy = i.ToColumn<string>("SUBFUND CCY"),
    SecurityCcy = i.ToColumn<string>("SECURITY CCY"),
    Currency = i.ToColumn<string>("CURRENCY"),
    Quantity = i.ToNumberColumn<double>("QUANTITY", "."),
    MarketValueInFdCcy = i.ToNumberColumn<double>("MARKET VALUE IN FD CCY", "."),
    MarketValueInSecCcy = i.ToNumberColumn<double>("MARKET VALUE IN SEC CCY", "."),
    LastCouponDate = i.ToOptionalDateColumn("LAST COUPON DATE", "yyyyMMdd"),
    NextCouponDate = i.ToOptionalDateColumn("NEXT COUPON DATE", "yyyyMMdd"),
    InvestmentType = i.ToColumn<string>("INVESTMENT TYPE"),
    GeographicalSector = i.ToColumn<string>("GEOGRAPHICAL SECTOR"),
    BookCost = i.ToNumberColumn<double?>("BOOK COST", "."),
    BookCostInFundCcy = i.ToNumberColumn<double?>("BOOK COST IN FUND CCY", "."),
    ProfitLossOnExchange = i.ToNumberColumn<double?>("PROFIT/LOSS ON EXCHANGE", "."),
    ProfitLossOnMarket = i.ToNumberColumn<double?>("PROFIT/LOSS ON MARKET", "."),
    OptionStyle = i.ToColumn<string>("STYLE"),
    PutOrCall = i.ToColumn<string>("OPTION/FUTURE TYPE"),
    CostPrice = i.ToNumberColumn<double?>("COST PRICE", "."),
    ValuationPrice = i.ToNumberColumn<double?>("VALUATION PRICE", "."),
    NumberOfAccruedDays = i.ToNumberColumn<int?>("NUMBER OF ACCRUED DAYS", "."),
    AccruedInt = i.ToNumberColumn<double?>("ACCRUED INT.", "."),
    AccruedIntFdCcy = i.ToNumberColumn<double?>("ACCRUED INT FD CCY", "."),
    StrikePrice = i.ToNumberColumn<double?>("STRIKE PRICE", "."),
    ContractSize = i.ToNumberColumn<double?>("CONTRACT SIZE", "."),
    InterestCalculationBasis = i.ToColumn("INTEREST CALCULATION BASIS"), // TODO: See how to import 365-6/360 (-> meaning of "-6"?)
    RowGuid = i.ToRowGuid(),
    FilePath = i.ToSourceName(),
    RowNumber = i.ToLineNumber(),
}).IsColumnSeparated(';');
#endregion

#region Streams
var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse position file", rbcPositionFileDefinition) //, i => i.RelativePath)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var targetSecuritiesFileStream = posFileStream
    .Fix($"{TaskName}: Fill in missing values for old files", o => o
        .FixProperty(i => i.SecurityCcy).IfNullWith(i => i.SubFundCcy)
        .FixProperty(i => i.OptionStyle).IfNullWith(i => "E")
        .FixProperty(i => i.PutOrCall).IfNullWith(i => putOrCall(i.InstrumentName))
        .FixProperty(i => i.GeographicalSector).IfNotNullWith(i => GetCountryIso2FromGeographicalSector(i.GeographicalSector))
    )
    .Select($"{TaskName}: Define security type", row =>
    {
        var typeCode = GetSecurityTypeAndCode(row.InvestmentType, row.SubFundCcy, row.AccountNumber, row.InternalNumber,row.IsinCode);
        return new
        {
            SecurityType = typeCode.type,
            InternalCode = typeCode.code,
            FileRow = row
        };
    });
#endregion

#region Target SICAVs + SubFunds

var targetSicavsStream = targetSecuritiesFileStream
    .Where($"{TaskName}:Keep only Share Class securities", i=> i.SecurityType == ImportedSecurityType.ShareClass)
    .Distinct($"{TaskName}: distinct SecBase SICAV", i => GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName))
    .LookupCountry($"{TaskName}: Get security SICAV related country", 
            i => i.FileRow.GeographicalSector, (l, r) => new { FileRow = l.FileRow, Country = r })
     .LookupCurrency($"{TaskName}: get related SICAV currency", i => i.FileRow.SecurityCcy , 
        (l,r) => new {FileRow = l.FileRow, Country=l.Country, Currency = r })
    .Select($"{TaskName}: Create Issuer SICAV", i => new Sicav{
        InternalCode = GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName),
        Name = GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName),
        CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: save target issuer Sicavs", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSicavIssuersStream = targetSicavsStream
    .Fix($"{TaskName}: IssuerId ", i => i.FixProperty(i => i.IssuerId).AlwaysWith(i => i.Id))
    .EfCoreSave("Fixing Sicav issuer Id");

var targetSubFundsStream = targetSecuritiesFileStream
    .Where($"{TaskName}:Keep only Share Class securities for sub fund", i=> i.SecurityType == ImportedSecurityType.ShareClass)
    .Distinct($"{TaskName}: distinct SecBase Sub-Funds", i => GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName))
    .Lookup($"{TaskName}: get related sub-fund Sicav", targetSicavsStream,
            i => GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName), i => i.InternalCode,
            (l,r) => new {FileRow = l.FileRow,InternalCode = l.InternalCode, Sicav = r })
    .LookupCountry($"{TaskName}: get related sub fund country", 
            i => i.FileRow.GeographicalSector, 
            (l, r) => new {FileRow = l.FileRow,InternalCode = l.InternalCode, Sicav = l.Sicav, Country = r })
    .LookupCurrency($"{TaskName}: get related sub fund currency", i => i.FileRow.SecurityCcy , 
        (l,r) => new {FileRow = l.FileRow,InternalCode = l.InternalCode, Sicav = l.Sicav, Country=l.Country, Currency = r })
    .Select($"{TaskName}: Create target subFund ", i => new SubFund{
        InternalCode =  GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName),
        Name =  GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName),
        ShortName  =  "from RBC files",
        CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        DomicileId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,
        SicavId = i.Sicav.Id,
        // SettlementNbDays = i.FileRow.FundValRdmpt,
        // CutOffTime = TimeSpan.TryParse(i.FileRow.TechCutoff,out var res)?res: (TimeSpan?) null, //<TechCutoff>14:15</TechCutoff>
        PricingFrequency = FrequencyType.Daily,
    })
    .EfCoreSave($"{TaskName}: save target sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());
#endregion

#region Issuers
var issuerCompaniesStream = targetSecuritiesFileStream
    .Where($"{TaskName}: Exclude Share Classes and cash", i=> i.SecurityType != ImportedSecurityType.ShareClass 
                && i.SecurityType !=ImportedSecurityType.Cash)
    .Distinct($"{TaskName}: distinct SecBase Issuers Companies", i =>  GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName))
    .LookupCountry($"{TaskName}: Get security issuer country", 
            i => i.FileRow.GeographicalSector, (l, r) => new { FileRow = l.FileRow, Country = r })
    .LookupCurrency($"{TaskName}: get related issuer currency", i => i.FileRow.SecurityCcy , 
        (l,r) => new {FileRow = l.FileRow, Country=l.Country, Currency = r })
    .Select($"{TaskName}: Create Issuer companies",i=> new Company{
        InternalCode = GetIssuerNameFromSecurityName(i.FileRow.InstrumentName),
        Name = GetIssuerNameFromSecurityName(i.FileRow.InstrumentName),
        CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());
#endregion

#region Target Securities (Sec + Cash)
//Cash Issuer
var cashIssuerStream = ProcessContextStream
    .Select($"{TaskName}: Create create RBC as cash issuer", 
        ctx => new Company { InternalCode = "20009", Name = "RBC Investor Services Luxembourg",Regulated=true,Culture=new CultureInfo("en")})
    .EfCoreSave($"{TaskName}: Save RBC as cash issuer", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure RBC as cash issuer is single");

// Create Create Cash Securities
var cashStream = targetSecuritiesFileStream
    .Where($"{TaskName}: Keep only cash", s => s.SecurityType == ImportedSecurityType.Cash)
    .Distinct($"{TaskName}: Distinct cash", c => c.InternalCode)
    .LookupCurrency($"{TaskName}: Get cash related currency", i => i.FileRow.SecurityCcy, (l, r) => new { l.FileRow, Currency = r })
    .Select($"{TaskName}: Create cash",cashIssuerStream, (s,ci) => CreateCash(s.FileRow.InstrumentName, s.Currency?.Id, 
            s.FileRow.SecurityCcy, s.FileRow.AccountNumber, ci !=null? ci.Id : (int?)null ))
    .EfCoreSave($"{TaskName}: Insert cash", o => o.SeekOn(c => c.InternalCode).Output((i, e) => (Security)e).DoNotUpdateIfExists());

// Create Target Securities
var targetSecuritiesStream = targetSecuritiesFileStream
    .Where($"{TaskName}: Keep only security instrument", s => s.SecurityType != ImportedSecurityType.Cash)
    .Distinct($"{TaskName}: Pre distinct security instrument", i => i.InternalCode)
    .LookupCurrency($"{TaskName}: Get security instrument related currency", i => i.FileRow.SecurityCcy, (l, r) => new { l.SecurityType, l.FileRow, l.InternalCode, Currency = r })
    .LookupCountry($"{TaskName}: Get security instrument related country", i => i.FileRow.GeographicalSector, 
        (l, r) => new { l.SecurityType, l.FileRow, l.InternalCode, l.Currency, Country = r })
    .Lookup($"{TaskName}: Lookup target sub-fund",targetSubFundsStream, 
        i => GetSubFundNameFromTargetShareClassName(i.FileRow.InstrumentName), i => i.InternalCode,
        (l,r) => new { l.SecurityType, l.FileRow, l.InternalCode, l.Currency, l.Country, TargetSubFund = r }) 
    .Lookup($"{TaskName}: Lookup target issuer", issuerCompaniesStream, 
        i =>  GetIssuerNameFromSecurityName(i.FileRow.InstrumentName), i => i.InternalCode,
        (l,r) => new { l.SecurityType, l.FileRow, l.InternalCode, l.Currency, l.Country, l.TargetSubFund, Issuer = r })
    .Select($"{TaskName}: Create target securities", i => CreateTargetSecurity(i.SecurityType, i.Currency?.Id, i.FileRow.IsinCode, 
                    i.FileRow.InstrumentName, i.InternalCode, i.FileRow.NextCouponDate, i.FileRow.MaturityDate, 
                    i.FileRow.QuotationPlace, i.FileRow.PutOrCall, i.FileRow.OptionStyle, i.Country?.Id, 
                    i.FileRow.ContractSize, i.FileRow.StrikePrice, i.FileRow.LastCouponDate,i.Issuer,i.TargetSubFund,i.FileRow.InternalNumber))
    .EfCoreSave($"{TaskName}: Save target securities", o => o.SeekOn(c => c.Isin).AlternativelySeekOn(c => c.InternalCode)
        .Output((i, e) => (Security)e).DoNotUpdateIfExists());
#endregion

#region Target Security Classifications
// Create ClassificationType
var classificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create RBC classification type", ctx => new SecurityClassificationType { Code = "RBC Economic Sector", Name = new MultiCultureString { ["en"] = "RBC Economic Sector" } })
    .EfCoreSave($"{TaskName}: Save RBC classification type", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure RBC classification type is single");

// Create Classification
var classificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct classification", i => i.EconomicSectorLabel)
    .Select($"{TaskName}: Get related classification type", classificationTypeStream, (i, ct) => new Classification
    {
        Code = i.EconomicSectorLabel,
        Name = new MultiCultureString { ["en"] = System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EconomicSectorLabel.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save RBC classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// ClassificationOfSecurity
var classificationOfSecurityStream = targetSecuritiesStream
    .CorrelateToSingle($"{TaskName}: Get related security classification", classificationStream, 
        (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id })
    .EfCoreSave($"{TaskName}: Insert security classification", o => o.SeekOn(i => new { i.SecurityId, i.ClassificationTypeId }).DoNotUpdateIfExists());
#endregion


#region Portfolios/SubFund
var luxembourgCountry = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get Luxembourg Country", (ctx, j) => ctx.Set<Country>().Where(c => c.IsoCode2 == "LU"))
    .EnsureSingle($"{TaskName}: ensures only one LU Country");

var sicavStream = posFileStream
    .Distinct($"{TaskName}: Distinct sicavs", i => SplitFundName(i.FundName).SicavName)
    .LookupCurrency($"{TaskName}: Lookup SICAV reference currency",i=>i.Currency, (l,r)=> new {FileRow = l, Currency = r})  
    .Select($"{TaskName}: Get Sub-Fund Luxembourg Country",luxembourgCountry, (l,r) => new {FileRow = l.FileRow, Currency = l.Currency, Country = r })  
    .Select($"{TaskName}: Create sicavs", ProcessContextStream, (i, ctx) => new Sicav
    {
        InternalCode = SplitFundName(i.FileRow.FundName).SicavName,
        Name = SplitFundName(i.FileRow.FundName).SicavName,
        IssuerId = ctx.TenantId,        
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,
        CountryId = i.Country.Id,
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        LegalForm = LegalForm.SICAV,
        Regulated = true,
    })
    .EfCoreSave($"{TaskName}: Save SICAVs", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var portfoliosStream = posFileStream
    .Distinct($"{TaskName}: Distinct SubFund", i => i.FundCode)
    .LookupCurrency($"{TaskName}: Get related currency for portfolio", l => l.Currency, (l, r) => new { FileRow = l, Currency = r })
    .Select($"{TaskName}: Get Luxembourg Country",luxembourgCountry, (l,r) => new {FileRow = l.FileRow, Currency = l.Currency, Country = r })
    .CorrelateToSingle($"{TaskName}: Get related sicav", sicavStream, (l, r) => new { l.FileRow, l.Currency, l.Country, Sicav = r })
    .Select($"{TaskName}: Create SubFund", ProcessContextStream, (i, ctx) => new SubFund
    {
        InternalCode = i.FileRow.FundCode,
        Name = SplitFundName(i.FileRow.FundName).FundName,
        ShortName = SplitFundName(i.FileRow.FundName).FundName.Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        CountryId = i.Country.Id,
        DomicileId = i.Country.Id,
        SicavId = i.Sicav?.Id,
        PricingFrequency = FrequencyType.Daily,
        CutOffTime = TimeSpan.TryParse("14:00",out var res)?res: (TimeSpan?) null,
        SettlementNbDays = 3,
    })
    .EfCoreSave($"{TaskName}: Save SubFunds", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

#endregion

#region Positions

// Create Compositions
var compositionStream = posFileStream
    .Distinct($"{TaskName}: Distinct composition for a date", i => new { i.FundCode, i.NavDate })
    .Lookup($"{TaskName}: Lookup for portfolio", portfoliosStream ,i => i.FundCode, i => i.InternalCode, 
        (l, r) => new { FileRow = l, Portfolio = r })
    .Select($"{TaskName}: Create composition", i => new {
        FundCode = i.FileRow.FundCode,
        Composition = new PortfolioComposition { PortfolioId = i.Portfolio.Id, Date = i.FileRow.NavDate }
    })
    .EfCoreSave($"{TaskName}: Insert composition", o => 
        o.Entity(i=>i.Composition).SeekOn(i => new {i.PortfolioId,i.Date}).DoNotUpdateIfExists().Output((i,e)=> i));

var allSavedSecurities = targetSecuritiesStream.UnionAll($"{TaskName}: Join cash and instrument types", cashStream);

// Create Positions
var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: Get related security", allSavedSecurities, 
        (l, r) => new { FileRow = l, Security = r })
    .Lookup($"{TaskName}: Get related composition",compositionStream, 
        i => new { FundCode = i.FileRow.FundCode, Date = i.FileRow.NavDate}, i => new { FundCode = i.FundCode, Date = i.Composition.Date},
        (l, r) => new { FileRow = l.FileRow, Security = l.Security, Composition = r.Composition })
    .Aggregate($"{TaskName}: Sum positions duplicates within a file",
        i => new
        {
            i.FileRow.FilePath,
            SecurityId = i.Security != null? i.Security.Id : throw new Exception($"Create position: Security not found: {i.FileRow.FundCode} - {i.FileRow.InstrumentName}" ),
            CompositionId= i.Composition !=null? i.Composition.Id  : throw new Exception($"Create position: composition not found: {i.FileRow.FundCode} - {i.FileRow.InstrumentName}"),
        },
        i => new
        {
            Quantity = (double) 0,
            MarketValueInFdCcy = (double) 0,
            MarketValueInSecCcy = (double) 0,
            // PercNav = (double) 0,
            AccruedIntFdCcy = (double) 0,
            AccruedInt = (double) 0,
            CostPrice = (double) 0,
            BookCostInFundCcy = (double) 0,
            BookCost = (double) 0,
            NumberOfAccruedDays = (int?) null,
            ProfitLossOnExchange = (double) 0,
            ProfitLossOnMarket = (double) 0,
            Count = 0,
        },
        (a, v) => new
        {
            Quantity = a.Quantity + v.FileRow.Quantity,
            MarketValueInFdCcy = a.MarketValueInFdCcy + v.FileRow.MarketValueInFdCcy,
            MarketValueInSecCcy = a.MarketValueInSecCcy + v.FileRow.MarketValueInSecCcy,
            // PercNav = a.Values.PercNav + v.FileRow.PercNav,
            AccruedIntFdCcy = a.AccruedIntFdCcy + v.FileRow.AccruedIntFdCcy ?? 0,
            AccruedInt = a.AccruedInt + v.FileRow.AccruedInt ?? 0,
            CostPrice = a.CostPrice + v.FileRow.CostPrice ?? 0,
            BookCostInFundCcy = a.BookCostInFundCcy + v.FileRow.BookCostInFundCcy ?? 0,
            BookCost = a.BookCost + v.FileRow.BookCost ?? 0,
            NumberOfAccruedDays = v.FileRow.NumberOfAccruedDays,
            ProfitLossOnExchange = a.ProfitLossOnExchange + v.FileRow.ProfitLossOnExchange ?? 0,
            ProfitLossOnMarket = a.ProfitLossOnMarket + v.FileRow.ProfitLossOnMarket ?? 0,
            Count = a.Count + 1,
        })
    .Select($"{TaskName}: Get position aggregation",
        i => new
        {
            i.Key.CompositionId,
            i.Key.SecurityId,
            Quantity = i.Aggregation.Quantity,
            MarketValueInFdCcy = i.Aggregation.MarketValueInFdCcy,
            MarketValueInSecCcy = i.Aggregation.MarketValueInSecCcy,
            // PercNav = i.Aggregation.Values.PercNav,
            AccruedIntFdCcy = i.Aggregation.AccruedIntFdCcy / i.Aggregation.Count,
            AccruedInt = i.Aggregation.AccruedInt / i.Aggregation.Count,
            CostPrice = i.Aggregation.CostPrice / i.Aggregation.Count,
            BookCostInFundCcy = i.Aggregation.BookCostInFundCcy,
            BookCost = i.Aggregation.BookCost,
            NumberOfAccruedDays = i.Aggregation.NumberOfAccruedDays,
            ProfitLossOnExchange = i.Aggregation.ProfitLossOnExchange,
            ProfitLossOnMarket = i.Aggregation.ProfitLossOnMarket,
        }
    )   
    .Select($"{TaskName}: Create positions",  i => new Position
    {
        PortfolioCompositionId = i.CompositionId,
        SecurityId = i.SecurityId,
        MarketValueInPortfolioCcy = i.MarketValueInFdCcy,
        MarketValueInSecurityCcy = i.MarketValueInSecCcy,        
        AccruedInterestInPortfolioCcy = i.AccruedIntFdCcy,
        AccruedInterestInSecurityCcy = i.AccruedInt,
        CostPrice = i.CostPrice,
        BookCostInPortfolioCcy = i.BookCostInFundCcy,
        BookCostInSecurityCcy = i.BookCost,
        NbAccruedDays = i.NumberOfAccruedDays,
        ProfitLossOnFxPortfolioCcy = i.ProfitLossOnExchange,
        ProfitLossOnMarketPortfolioCcy = i.ProfitLossOnMarket,
        Value = i.Quantity
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: Save position", o => o.SeekOn(i => new { i.PortfolioCompositionId, i.SecurityId }));
#endregion

return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream,classificationOfSecurityStream, 
        targetSubFundsStream,targetSicavsStream);

#region Helper Methods

string GetIssuerNameFromSecurityName(string securityName)
    => GetSubFundNameFromTargetShareClassName(securityName);

string GetSubFundNameFromTargetShareClassName(string shareClassName)
    => (shareClassName.Contains("-"))? shareClassName.Split("-")[0].Trim() 
        : string.Join(" ", shareClassName.Split().Take(3));

(string FundName, string SicavName) SplitFundName(string name)
{
    var idx = name.IndexOf("-");
    if (idx < 0) return (name, (string)null);
    return (name.Substring(idx + 1).Trim(), name.Substring(0, idx).Trim());
}

enum ImportedSecurityType
{
    Equity,
    Etf,
    ShareClass,
    Bond,
    Option,
    Future,
    Cash
}
string putOrCall(string instrumentName)
{
    if (instrumentName.StartsWith("put", StringComparison.InvariantCultureIgnoreCase))
        return "P";
    if (instrumentName.StartsWith("call", StringComparison.InvariantCultureIgnoreCase))
        return "C";
    return (string)null;
}
(ImportedSecurityType type, string code) GetSecurityTypeAndCode(string investmentType, string subFundCurrencyCode, 
                                                                string accountNumber, string rbcInternalCode, string isin)
{
    var splitted = (investmentType ?? "").Split(':');
    var rbcCode = string.IsNullOrWhiteSpace(investmentType) ? "" : splitted[0].Trim();
    string internalCode = !string.IsNullOrEmpty(isin)? isin : rbcInternalCode;

    switch (rbcCode)
    {
        case "100": // SHARES
        case "102": //
        case "103": //
        case "111": //
        case "120": //
        case "117": // REITS
        case "118": // NON G.T. REITS
        case "411": // SICAF
            return (ImportedSecurityType.Equity, internalCode);
        case "410":
            return (ImportedSecurityType.Etf, internalCode);
        case "484": // Investment Funds - UCITS- French
        case "485": // Investment Funds - UCITS- European
            return (ImportedSecurityType.ShareClass, internalCode);
        case "200": // STRAIGHT BONDS
        case "201": // FLOATING RATE BONDS
        case "270": // Commercial paper
        case "271": // Certificate of Deposit
        case "202": // "202 : ZERO COUPON BONDS"
            return (ImportedSecurityType.Bond, internalCode);
        case "603": // Call/Put
            var derivativeType = splitted[1].TrimStart();
            if (derivativeType.StartsWith("option", true, System.Globalization.CultureInfo.InvariantCulture))
                return (ImportedSecurityType.Option, rbcInternalCode);
                
            else if (derivativeType.StartsWith("future", true, System.Globalization.CultureInfo.InvariantCulture))
                return (ImportedSecurityType.Future, rbcInternalCode);
            break;
        case "850": // ACCRUED EXP.
        case "650": // PREPAID EXP.
        case "450": // CASH
        case "600": // RECEIVABLES
        case "800": // PAYABLES
        case "670": // FORMATION EXP.
        default:
            return (ImportedSecurityType.Cash, $"{accountNumber}-{subFundCurrencyCode}");
    }
    return (ImportedSecurityType.Cash, $"{accountNumber}-{subFundCurrencyCode}");
}

DividendDistributionPolicy? GetDividendPolicyFromName(string shareClassName)
{
    if (shareClassName.Split('-').Length>1)
    {
        string tmp=shareClassName.Split('-').Last().ToLower();
        if (tmp.Contains("dis"))
            return DividendDistributionPolicy.Distribution;
        if (tmp.Contains("cap"))
            return DividendDistributionPolicy.Accumulation;    
        if (tmp.Contains("ac"))
            return DividendDistributionPolicy.Accumulation;    
    }
    return null;
}
InvestorType? GetInvestorTypeFromName(string shareClassName)
=> shareClassName.ToUpper().Split('-').Skip(1).Any(i=> i == "I")? InvestorType.Institutional : 
    shareClassName.ToUpper().Split('-').Skip(1).Any(i=> i == "R")? InvestorType.Retail : null;
  

SecurityInstrument CreateTargetSecurity(ImportedSecurityType type, int? currencyId, string isin,
    string instrumentName, string internalCode,DateTime? nextCouponDate, DateTime? maturityDate,string quotationPlace,
    string putOrCall,string optionStyle,int? countryId,
    double? contractSize, double? strikePrice, DateTime? lastCouponDate, Company issuer,SubFund targetSubFund, string secRbcCode)
{
    switch (type)
    {
        case ImportedSecurityType.Equity:
            return new Equity
            {
                Isin = isin,
                CountryId = countryId,
                PricingFrequency = FrequencyType.Daily,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = secRbcCode,
                IssuerId = issuer != null? issuer.Id : (int?) null,
            };
        case ImportedSecurityType.Etf:
            return new Etf
            {
                Isin = isin,
                CountryId = countryId,
                PricingFrequency = FrequencyType.Daily,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = secRbcCode,
                IssuerId = issuer != null? issuer.Id : (int?) null,
            };
        case ImportedSecurityType.ShareClass:
            return new ShareClass
            {
                Isin = isin,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = secRbcCode,
                DividendDistributionPolicy = GetDividendPolicyFromName(instrumentName),
                InvestorType = GetInvestorTypeFromName(instrumentName),
                SubFundId = targetSubFund != null? targetSubFund.Id : (int?) null,
            };
        case ImportedSecurityType.Bond:
            return new Bond
            {
                Isin = isin,
                CountryId = countryId,
                PricingFrequency = FrequencyType.Daily,
                NextCouponDate = nextCouponDate,
                PreviousCouponDate = lastCouponDate,
                MaturityDate = maturityDate,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = secRbcCode,
                IssuerId = issuer != null? issuer.Id : (int?) null,
            };
        case ImportedSecurityType.Option: // Call/Put
            return new Option
            {
                UnderlyingIsin = isin,
                CountryId = countryId,
                PricingFrequency = FrequencyType.Daily,
                Type = string.Equals(optionStyle, "E", StringComparison.InvariantCultureIgnoreCase) ? OptionType.European : OptionType.American,
                MaturityDate = maturityDate,
                IsOtc = quotationPlace == null ? false : string.Equals(quotationPlace, "otc", StringComparison.InvariantCultureIgnoreCase),
                PutCall = string.Equals(putOrCall, "P", StringComparison.InvariantCultureIgnoreCase) ? PutCall.Put : PutCall.Call,
                StrikePrice = strikePrice,
                ContractSize = contractSize,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = secRbcCode,
                IssuerId = issuer != null? issuer.Id : (int?) null,
            };
        case ImportedSecurityType.Future: // Call/Put
            return new Future
            {
                UnderlyingIsin = isin,
                CountryId = countryId,
                PricingFrequency = FrequencyType.Daily,
                MaturityDate = maturityDate,
                IsOtc = quotationPlace == null ? false : string.Equals(quotationPlace, "otc", StringComparison.InvariantCultureIgnoreCase),
                StrikePrice = strikePrice,
                ContractSize = contractSize,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = secRbcCode,
                IssuerId = issuer != null? issuer.Id : (int?) null,
            };
    }
    return null;
}

Cash CreateCash(string instrumentName,int? subFundCurrencyId,string subFundCurrencyCode,string accountNumber, int? issuerId)
{
    return new Cash
    {
        Name = $"{instrumentName} ({subFundCurrencyCode})",
        ShortName = $"{instrumentName} ({subFundCurrencyCode})".Truncate(MaxLengths.ShortName),
        CurrencyId = subFundCurrencyId,
        InternalCode = $"{accountNumber}-{subFundCurrencyCode}",
        IssuerId = issuerId,
    };
}

string GetCountryIso2FromGeographicalSector(string countryRbcName)
{
    var countryDic = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
    {
        ["GERMANY"] = "DE",
        ["PORTUGAL"] = "PT",
        ["NETHERLANDS"] = "NL",
        ["GREAT BRITAIN"] = "GB",
        ["SPAIN"] = "ES",
        ["LUXEMBOURG"] = "LU",
        ["FRANCE"] = "FR",
        ["BELGIUM"] = "BE",
        ["AUSTRIA"] = "AT",
        ["FINLAND"] = "FI",
        ["DENMARK"] = "DK",
        ["SWITZERLAND"] = "CH",
        ["SWEDEN"] = "SE",
        ["NORWAY"] = "NO",
        ["MEXICO"] = "MX",
        ["ICELAND"] = "IS",
        ["ITALY"] = "IT",
        ["JERSEY"] = "JE",
        ["IRELAND"] = "IE",
        ["SINGAPORE"] = "SG",
        ["GUERNSEY"] = "GG",
        ["CHINA"] = "CN",
        ["UNITED STATES (U.S.A.)"] = "US",
        ["MAN (ISLE OF)"] = "IM",
        ["ISRAEL"] = "IL",
        ["PANAMA"] = "PA",
        ["CANADA"] = "CA",
        ["HONG KONG"] = "HK",
        ["JAPAN"] = "JP",
        ["CAYMAN ISLANDS"] = "KY",
        ["CZECH"] = "CZ",
        ["BERMUDA ISLANDS"] = "BM",
        ["SOUTH AFRICA"] = "ZA",
        ["AUSTRALIA"] = "AU",
    };

    if (!countryDic.ContainsKey(countryRbcName))
        throw new Exception("Country not mapped in RBC country dictionary: " + countryRbcName);
    return countryDic[countryRbcName];
}
#endregion
