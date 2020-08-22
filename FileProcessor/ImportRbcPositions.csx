Dictionary<string, string> fromGeographicalSectorToAlpha2Dictionary = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
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



var rbcPositionFileDefinition = FlatFileDefinition.Create(i => new
{
    // PercNav = i.ToNumberColumn<double>("% NAV", "."),
    FundCode = i.ToColumn<string>("FUND CODE"),
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
    FilePath = i.ToSourceName(),
    RowNumber = i.ToLineNumber(),
    CostPrice = i.ToNumberColumn<double?>("COST PRICE", "."),
    ValuationPrice = i.ToNumberColumn<double?>("VALUATION PRICE", "."),
    NumberOfAccruedDays = i.ToNumberColumn<int?>("NUMBER OF ACCRUED DAYS", "."),
    AccruedInt = i.ToNumberColumn<double?>("ACCRUED INT.", "."),
    AccruedIntFdCcy = i.ToNumberColumn<double?>("ACCRUED INT FD CCY", "."),
    StrikePrice = i.ToNumberColumn<double?>("STRIKE PRICE", "."),
    ContractSize = i.ToNumberColumn<double?>("CONTRACT SIZE", "."),
    InterestCalculationBasis = i.ToColumn("INTEREST CALCULATION BASIS"), // TODO: See how to import 365-6/360 (-> meaning of "-6"?)
    RowGuid = i.ToRowGuid()
}).IsColumnSeparated(',');

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse position file", rbcPositionFileDefinition) //, i => i.RelativePath)
    .SetForCorrelation($"{TaskName}: Set correlation key");


// SecurityClassificationType
var classificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create RBC classification type", ctx => new SecurityClassificationType { Code = "RBC Economic Sector", Name = new MultiCultureString { ["en"] = "RBC Economic Sector" } })
    .EfCoreSave($"{TaskName}: Save RBC classification type", o => o.SeekOn(ct => ct.Code))
    .EnsureSingle($"{TaskName}: Ensure RBC classification type is single");

// SecurityClassification
var classificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct classification", i => i.EconomicSectorLabel)
    .Select($"{TaskName}: Get related classification type", classificationTypeStream, (i, ct) => new SecurityClassification
    {
        Code = i.EconomicSectorLabel,
        Name = new MultiCultureString { ["en"] = System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EconomicSectorLabel.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save RBC classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }));

string putOrCall(string instrumentName)
{
    if (instrumentName.StartsWith("put", StringComparison.InvariantCultureIgnoreCase))
        return "P";
    if (instrumentName.StartsWith("call", StringComparison.InvariantCultureIgnoreCase))
        return "C";
    return (string)null;
}

var allSecuritiesStream = posFileStream
    .Fix($"{TaskName}: Fill in missing values for old files", o => o
        .FixProperty(i => i.SecurityCcy).IfNullWith(i => i.SubFundCcy)
        .FixProperty(i => i.OptionStyle).IfNullWith(i => "E")
        .FixProperty(i => i.PutOrCall).IfNullWith(i => putOrCall(i.InstrumentName))
        .FixProperty(i => i.GeographicalSector).IfNotNullWith(i => fromGeographicalSectorToAlpha2Dictionary[i.GeographicalSector])
    )
    .Select($"{TaskName}: Define security type", row =>
    {
        var typeCode = GetSecurityTypeAndCode(row.InvestmentType, row.SubFundCcy, row.AccountNumber, row.InternalNumber);
        return new
        {
            SecurityType = typeCode.type,
            InternalCode = typeCode.code,
            FileRow = row
        };
    });

// Cash
var cashStream = allSecuritiesStream
    .Where($"{TaskName}: Keep only cash", s => s.SecurityType == ImportedSecurityType.Cash)
    .Distinct($"{TaskName}: Distinct cash", c => c.InternalCode)
    .LookupCurrency($"{TaskName}: Get cash related currency", i => i.FileRow.SecurityCcy, (l, r) => new { l.FileRow, Currency = r })
    .Select($"{TaskName}: Create cash", s => CreateCash(s.FileRow.InstrumentName, s.Currency?.Id, s.FileRow.SecurityCcy, s.FileRow.AccountNumber))
    .EfCoreSave($"{TaskName}: Insert cash", o => o.SeekOn(c => c.InternalCode).Output((i, e) => (Security)e).DoNotUpdateIfExists());

// SecurityInstrument
var securityInstrumentStream = allSecuritiesStream
    .Where($"{TaskName}: Keep only security instrument", s => s.SecurityType != ImportedSecurityType.Cash)
    .Distinct($"{TaskName}: Pre distinct security instrument", i => new { i.FileRow.IsinCode, i.InternalCode })
    .LookupCurrency($"{TaskName}: Get security instrument related currency", i => i.FileRow.SecurityCcy, (l, r) => new { l.SecurityType, l.FileRow, Currency = r })
    .LookupCountry($"{TaskName}: Get security instrument related country", i => i.FileRow.GeographicalSector, (l, r) => new { l.SecurityType, l.FileRow, l.Currency, Country = r })
    .Select($"{TaskName}: Create security instrument", i => CreateSecurityInstrument(i.SecurityType, i.Currency?.Id, i.FileRow.IsinCode, i.FileRow.InstrumentName, i.FileRow.InternalNumber, i.FileRow.NextCouponDate, i.FileRow.MaturityDate, i.FileRow.QuotationPlace, i.FileRow.PutOrCall, i.FileRow.OptionStyle, i.Country?.Id, i.FileRow.ContractSize, i.FileRow.StrikePrice, i.FileRow.LastCouponDate))
    .ReKey($"{TaskName}: Uniformize target security codes", i => new { i.Isin, i.InternalCode }, (i, k) =>
    {
        i.Isin = k.Isin;
        i.InternalCode = k.InternalCode;
        return i;
    })
    .Distinct($"{TaskName}: Distinct security instrument", c => c.InternalCode)
    .EfCoreSave($"{TaskName}: Insert security instrument", o => o.SeekOn(c => c.Isin).AlternativelySeekOn(c => c.InternalCode).Output((i, e) => (Security)e).DoNotUpdateIfExists());

// ClassificationOfSecurity
var classificationOfSecurityStream = securityInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related security classification", classificationStream, (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id })
    .EfCoreSave($"{TaskName}: Insert classification of security", o => o.SeekOn(i => new { i.SecurityId, i.ClassificationTypeId }).DoNotUpdateIfExists());

// PortfolioComposition
var compositionStream = posFileStream
    .Distinct($"{TaskName}: Distinct composition for a date", i => new { i.FundCode, i.NavDate })
    .LookupPortfolio($"{TaskName}: Lookup for portfolio", i => i.FundCode, (l, r) => new { FileRow = l, Portfolio = r })
    .Select($"{TaskName}: Create composition", i => new PortfolioComposition { PortfolioId = i.Portfolio?.Id ?? 0, Date = i.FileRow.NavDate })
    .Where($"{TaskName}: Exclude composition with not found portfolio", i => i.PortfolioId != 0)
    .EfCoreSave($"{TaskName}: Insert composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }).DoNotUpdateIfExists());

var allSavedSecurities = securityInstrumentStream
    .UnionAll($"{TaskName}: Join cash and instrument types", cashStream);

// Position
var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: Get related security", allSavedSecurities, (l, r) => new { FileRow = l, Security = r })
    .Aggregate($"{TaskName}: Sum positions duplicates within a file",
        i => new
        {
            i.FileRow.FilePath,
            i.FileRow.NavDate,
            i.FileRow.FundCode,
            SecurityId = i.Security.Id
        },
        i => new
        {
            Quantity = (double)0,
            MarketValueInFdCcy = (double)0,
            MarketValueInSecCcy = (double)0,
            // PercNav = (double)0,
            AccruedIntFdCcy = (double)0,
            AccruedInt = (double)0,
            CostPrice = (double)0,
            BookCostInFundCcy = (double)0,
            BookCost = (double)0,
            NumberOfAccruedDays = (int?)null,
            ProfitLossOnExchange = (double)0,
            ProfitLossOnMarket = (double)0,
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
    .CorrelateToSingle($"{TaskName}: Get related composition", compositionStream, (i, r) => new Position
    {
        SecurityId = i.SecurityId,
        MarketValueInPortfolioCcy = i.MarketValueInFdCcy,
        MarketValueInSecurityCcy = i.MarketValueInSecCcy,
        PortfolioCompositionId = r.Id,
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
    .Distinct($"{TaskName}: Exclude positions duplicates", i => new { i.PortfolioCompositionId, i.SecurityId })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: Save position", o => o.SeekOn(i => new { i.PortfolioCompositionId, i.SecurityId }));
return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream, classificationOfSecurityStream);
#region Helper Methods
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
(ImportedSecurityType type, string code) GetSecurityTypeAndCode(string investmentType, string subFundCurrencyCode, string accountNumber, string internalCode)
{
    var splitted = (investmentType ?? "").Split(':');
    var rbcCode = string.IsNullOrWhiteSpace(investmentType) ? "" : splitted[0].Trim();
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
                return (ImportedSecurityType.Option, internalCode);
            else if (derivativeType.StartsWith("future", true, System.Globalization.CultureInfo.InvariantCulture))
                return (ImportedSecurityType.Future, internalCode);
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
SecurityInstrument CreateSecurityInstrument(
    ImportedSecurityType type,
    int? currencyId,
    string isin,
    string instrumentName,
    string internalCode,
    DateTime? nextCouponDate,
    DateTime? maturityDate,
    string quotationPlace,
    string putOrCall,
    string optionStyle,
    int? countryId,
    double? contractSize,
    double? strikePrice,
    DateTime? lastCouponDate)
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
                ShortName = instrumentName.Truncate(MaxLengths.ShortName)
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
                ShortName = instrumentName.Truncate(MaxLengths.ShortName)
            };
        case ImportedSecurityType.ShareClass:
            return new ShareClass
            {
                Isin = isin,
                InternalCode = internalCode,
                CurrencyId = currencyId,
                Name = instrumentName,
                ShortName = instrumentName.Truncate(MaxLengths.ShortName)
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
                ShortName = instrumentName.Truncate(MaxLengths.ShortName)
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
                ShortName = instrumentName.Truncate(MaxLengths.ShortName)
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
                ShortName = instrumentName.Truncate(MaxLengths.ShortName)
            };
    }
    return null;
}

Cash CreateCash(
    string instrumentName,
    int? subFundCurrencyId,
    string subFundCurrencyCode,
    string accountNumber)
{
    return new Cash
    {
        Name = $"{instrumentName} ({subFundCurrencyCode})",
        ShortName = $"{instrumentName} ({subFundCurrencyCode})".Truncate(MaxLengths.ShortName),
        CurrencyId = subFundCurrencyId,
        InternalCode = $"{accountNumber}-{subFundCurrencyCode}",
    };
}
#endregion
