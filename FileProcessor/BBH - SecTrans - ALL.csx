string getSecurityInternalCode(string Isin, string secId)
    => !string.IsNullOrWhiteSpace(Isin)? Isin : "BBH-"+secId;

//FILE MAPPING:
var fileDefinition = FlatFileDefinition.Create(i => new
{
    SecurityDesc = i.ToColumn("Security Desc"),  // SecurityDesc	Security Desc	BOOKING HOLDINGS INC
    Amount = i.ToNumberColumn<double?>("Amount", "."),  // Amount	Amount	106145.55
    Units = i.ToNumberColumn<double?>("Units", "."),    // Units	Units	56.0000
    UnitPrice = i.ToNumberColumn<double?>("Unit Price", "."), // UnitPrice	Unit Price	1893.950000
    Commission = i.ToNumberColumn<double?>("Commission", "."),  // Commission	Commission	53.03
    Principal = i.ToNumberColumn<double?>("Principal", "."),    // Principal	Principal	106061.20
    Type = i.ToColumn("Type"),  // Type	Type	RVP
    TransStatus = i.ToColumn("Trans Status"),  // TransStatus	Trans Status	CN
    SECFee = i.ToNumberColumn<double?>("SEC Fee", "."), // SECFee	SEC Fee	0.00
    CustodyFee = i.ToNumberColumn<double?>("Custody Fee", "."), // CustodyFee	Custody Fee	0.00
    Curr = i.ToColumn("Curr"),  // Curr	Curr	USD
    TradeDate = i.ToDateColumn("Trade Date","yyyy/MM/dd"),    // TradeDate	Trade Date	2020/01/29
    SettleDate = i.ToDateColumn("Settle Date","yyyy/MM/dd"),  // SettleDate	Settle Date	2020/01/31
    TradingBroker = i.ToColumn("Trading Broker"),  // TradingBroker	Trading Broker	BROWN BROTHERS HARRIMAN & CO
    AcctName = i.ToColumn("Acct Name"),  // AcctName	Acct Name	UI I - VALUFOCUS
    AcctNo = i.ToColumn("Acct No"),    // AcctNo	Acct No	6092696
    AssetType = i.ToColumn("Asset Type"),    // AssetType	Asset Type	STOCK
    CountryofOrigin = i.ToColumn("Country of Origin"),    // CountryofOrigin	Country of Origin	US
    ISIN = i.ToColumn("ISIN"),  // ISIN	ISIN	US09857L1089
    PaymentDate = i.ToDateColumn("Payment Date","yyyy/MM/dd"),    // PaymentDate	Payment Date	
    SecurityCategory = i.ToColumn("Security Category"),  // SecurityCategory	Security Category	EQUITY
    SecID = i.ToColumn("Sec ID"),  // SecID	Sec ID	09857L108
    SecurityMovementType = i.ToColumn("Security Movement Type"),  // SecurityMovementType	Security Movement Type	STANDARD TRADE
    Ticker = i.ToColumn("Ticker"),  // Ticker	Ticker	BKNG
    TradingBrokerID = i.ToColumn("Trading Broker ID"),    // TradingBrokerID	Trading Broker ID	00009010
    TransactionRefID = i.ToColumn("Transaction Ref ID"),   // TransactionRefID	Transaction Ref ID	1631091321000000000
    TransStatusDesc = i.ToColumn("Trans Status Desc"),  // TransStatusDesc	Trans Status Desc	CANCELED
    TransTypeDesc = i.ToColumn("Trans Type Desc"),    // TransTypeDesc	Trans Type Desc	RECEIVE VS PAYMENT
    ValueDate = i.ToColumn("Value Date"),  // ValueDate	Value Date	
}).IsColumnSeparated(',');

var secTransFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

#region broker and counterparties
var brokerCompanyStream = secTransFileStream
    .Where($"{TaskName} where tradingbroker has value", i=> !string.IsNullOrEmpty(i.TradingBroker))
    .Distinct($"{TaskName}: exclude duplicate brokers", i => i.TradingBroker)
    .Select($"{TaskName}: create broker companies", i => new Company
    {
        InternalCode = i.TradingBrokerID,
        Name = i.TradingBroker
    }).EfCoreSave($"{TaskName}: save broker companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var counterpartyStream = brokerCompanyStream
    .Select($"{TaskName}: create counterparty", euroCurrency, (i, j) => new CounterpartyRelationship
    {
        EntityId = i.Id,
        StartDate = DateTime.Today,
        CounterpartyType = CounterpartyType.Broker,
        CurrencyId = j.Id,
        EmirClassification = EmirClassification.Financial,
    }).EfCoreSave($"{TaskName}: save counterparty", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());
#endregion broker and counterparties

#region TargetSecurity

//Create TARGET SECURITIES
string getIssuerInternalCode(string name)
    => CultureInfo.InvariantCulture.TextInfo.ToTitleCase(name.ToLower()).Replace(" " ,"");

var issuerCompaniesStream = secTransFileStream    
    .Distinct($"{TaskName}: distinct issuers", i => getIssuerInternalCode(i.SecurityDesc))
    .LookupCurrency($"{TaskName}: get related company currency", i => i.Curr, 
        (l,r) => new {FileRow = l, Currency = r })
    // .LookupCountry($"{TaskName}: get issuer related country", l => l.CountryOfCompany, 
    //     (l,r) => new {FileRow = l, Country=r })
    .Select($"{TaskName}: Create Issuer companies",i=> new Company{
        InternalCode = getIssuerInternalCode(i.FileRow.SecurityDesc),
        Name = i.FileRow.SecurityDesc,
        //CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        CurrencyId = (i.Currency != null)? i.Currency.Id : (int?) null,        
        Culture = new CultureInfo("en"),
        YearEnd = new DateOfYear(12,31),
        Regulated = false,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSecurityInstrumentStream = secTransFileStream
    .Distinct($"{TaskName}: distinct positions security", i => getSecurityInternalCode(i.ISIN, i.SecID))
    .LookupCountry($"{TaskName}: get related country", i => GetCountryIsoFromBbhName(i.CountryofOrigin), 
        (l, r) => new { FileRow = l, Country = r })
    .LookupCurrency($"{TaskName}: get related currency", l => l.FileRow.Curr,
        (l, r) => new { l.FileRow, l.Country, Currency = r })
    .CorrelateToSingle($"{TaskName}: get related issuer",issuerCompaniesStream,
        (l,r) => new {l.FileRow, l.Country, l.Currency, Issuer = r})
    .Select($"{TaskName}: create instrument", i => 
        CreateSecurity(i.FileRow.SecID, i.FileRow.SecurityDesc,
            i.Issuer, i.FileRow.SecurityCategory, i.FileRow.ISIN, i.Currency, i.Country))
    .EfCoreSave($"{TaskName}: save target instrument", o => o
        .SeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    .Select($"{TaskName}: cast instrument into Security", i => i as Security);

#endregion

#region Create Security Transaction
var subFundStream = ProcessContextStream
        .EfCoreSelect($"{TaskName}: get portfolios stream", (ctx,j) => ctx.Set<Portfolio>());

var savedTransactionStream = secTransFileStream
    .Lookup($"{TaskName} get related portfolio",subFundStream, i => i.AcctName.Split(" - ").Last().ToLower().Trim(), i => i.Name.ToLower(),
            (l,r) => new { FileRow = l, Portfolio = r } )
    .CorrelateToSingle($"{TaskName}: get get related target security", targetSecurityInstrumentStream, 
        (l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r })
    // .EfCoreLookup($"{TaskName}: get related target security", 
    //     o => o.Set<SecurityInstrument>().On(i => getSecurityInternalCode(i.FileRow.ISIN,i.FileRow.SecID), i=> i.InternalCode)
    //     .Select((l, r) => new { l.FileRow, l.Portfolio, TargetSecurity = r }).CacheFullDataset())
    .CorrelateToSingle($"{TaskName}: get broker by internal code", counterpartyStream, 
        (l, r) => new { l.FileRow, l.Portfolio, l.TargetSecurity, Broker = r })
    .Select($"{TaskName}: Create security transaction", i => new SecurityTransaction
    {
        PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found: " + i.FileRow.AcctName),
        SecurityId = (i.TargetSecurity != null)? i.TargetSecurity.Id : throw new Exception("Security not found in database: "+i.FileRow.ISIN),
        OperationType = i.FileRow.Type == "RVP"? OperationType.Buy : (i.FileRow.Type == "DVP"? OperationType.Sale: throw new Exception("Unknown transaction type: " + i.FileRow.Type)),
        TransactionCode = "BBH-" + i.FileRow.TransactionRefID,
        Description = i.FileRow.SecurityDesc + " - " + i.FileRow.TransStatusDesc,
        TradeDate = i.FileRow.TradeDate,
        NavDate = i.FileRow.TradeDate,
        ValueDate = i.FileRow.TradeDate,
        SettlementDate = i.FileRow.SettleDate,
        Quantity = (i.FileRow.Units.HasValue)? i.FileRow.Units.Value 
                         : throw new Exception("Quantity not provided in Transaction: "+i.FileRow.TransactionRefID),
        FeesInSecurityCcy = i.FileRow.Commission,
        // //GrossAmountInPortfolioCcy = ...
        GrossAmountInSecurityCcy = (i.FileRow.Amount.HasValue)? i.FileRow.Amount.Value
                         : throw new Exception("Amount not provided in Transaction: "+i.FileRow.TransactionRefID),
        // //NetAmountInPortfolioCcy = ...
        NetAmountInSecurityCcy = i.FileRow.Principal,
        PriceInSecurityCcy = (i.FileRow.UnitPrice.HasValue)? i.FileRow.UnitPrice.Value 
                           : throw new Exception("UnitPrice not provided: "+ i.FileRow.TransactionRefID),
        BrokerId = (i.Broker != null)? i.Broker.Id : throw new Exception("Broker not found: " + i.FileRow.TradingBroker),
        TransactionType = TransactionType.SecurityMovement,
        DecisionType = TransactionDecisionType.Discretionary,
    }).EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());
#endregion

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", savedTransactionStream);


SecurityInstrument CreateSecurity(string securityCode, string secName, Company issuer, 
                        string secType, string isin, Currency currency, Country country)
{
    SecurityInstrument security = null;
    switch (secType.ToLower())
    {
        case "equity":
            security = new Equity();
            break;
        case "bond":
            security = new Bond();
            break;
        default:
            throw new Exception("Not implemented: " + secType);
    }
    security.InternalCode = getSecurityInternalCode(isin,securityCode);
    security.CurrencyId = (currency != null)? currency.Id : (int?) null;
    security.Name = secName;

    security.ShortName = security.Name.Truncate(MaxLengths.ShortName);

    if (security is SecurityInstrument securityInstrument)
        securityInstrument.Isin = isin;

    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.PricingFrequency = FrequencyType.Daily;
        regularSecurity.CountryId = (country != null)? country.Id : (int?) null;
        regularSecurity.IssuerId = (issuer != null)? issuer.Id : throw new Exception("Issuer not found for: " + secName); 
    }
    return security;
}

string GetCountryIsoFromBbhName(string countryName)
{
    Dictionary<string, string> fromCountryNameToAlpha2Dictionary = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
    {
        ["US"] = "US",
        ["USA"] = "US",
        ["JERSEYCI"] = "JE",
        ["FRANCE"] = "FR",
        ["BELGIUM"] = "BE",
        ["SPAIN"] = "ES",
        ["GERMANY"] = "DE",
        ["CURACAO"] = "CW",
        ["SWEDEN"] = "SE",
        ["CANADA"] = "CA",
        ["SWTZLAND"] = "CH",
        ["IRELAND"] = "IE",
        ["NETHLNDS"] = "NL",
        ["UTD KING"] = "GB",
        ["JAPAN"] = "JP",


        // ["Afghanistan"] = "AF",
        // ["Albania"] = "AL",
        // ["Algeria"] = "DZ",
        // ["American Samoa"] = "AS",
        // ["Andorra"] = "AD",
        // ["Angola"] = "AO",
        // ["Anguilla"] = "AI",
        // ["Antarctica"] = "AQ",
        // ["Antigua and Barbuda"] = "AG",
        // ["Argentina"] = "AR",
        // ["Armenia"] = "AM",
        // ["Aruba"] = "AW",
        // ["Ascension Island"] = "AC",
        // ["Australia"] = "AU",
        // ["Austria"] = "AT",
        // ["Azerbaijan"] = "AZ",
        // ["Bahamas"] = "BS",
        // ["Bahrain"] = "BH",
        // ["Bangladesh"] = "BD",
        // ["Barbados"] = "BB",
        // ["Belarus"] = "BY",
        
        // ["Belize"] = "BZ",
        // ["Benin"] = "BJ",
        // ["Bermuda"] = "BM",
        // ["Bhutan"] = "BT",
        // ["Bolivia"] = "BO",
        // ["Bosnia and Herzegovina"] = "BA",
        // ["Botswana"] = "BW",
        // ["Bouvet Island"] = "BV",
        // ["Brazil"] = "BR",
        // ["British Indian Ocean Territory"] = "IO",
        // ["Brunei Darussalam"] = "BN",
        // ["Bulgaria"] = "BG",
        // ["Burkina Faso"] = "BF",
        // ["Burundi"] = "BI",
        // ["Cabo Verde"] = "CV",
        // ["Cambodia"] = "KH",
        // ["Cameroon"] = "CM",
        
        // ["Canary Islands"] = "IC",
        // ["Cayman Islands"] = "KY",
        // ["Central African Republic"] = "CF",
        // ["Chad"] = "TD",
        // ["Chile"] = "CL",
        // ["China"] = "CN",
        // ["Christmas Island"] = "CX",
        // ["Clipperton Island"] = "CP",
        // ["Colombia"] = "CO",
        // ["Comoros"] = "KM",
        // ["Congo"] = "CG",
        // ["Cook Islands"] = "CK",
        // ["Costa Rica"] = "CR",
        // ["Cote d'Ivoire"] = "CI",
        // ["Croatia"] = "HR",
        // ["Cuba"] = "CU",        
        // ["Cyprus"] = "CY",
        // ["Czechia"] = "CZ",
        // ["Denmark"] = "DK",
        // ["Diego Garcia"] = "DG",
        // ["Djibouti"] = "DJ",
        // ["Dominica"] = "DM",
        // ["Dominican Republic"] = "DO",
        // ["Ecuador"] = "EC",
        // ["Egypt"] = "EG",
        // ["El Salvador"] = "SV",
        // ["Equatorial Guinea"] = "GQ",
        // ["Eritrea"] = "ER",
        // ["Estonia"] = "EE",
        // ["Eswatini"] = "SZ",
        // ["Ethiopia"] = "ET",
        // ["European Union"] = "EU",
        // ["Eurozone"] = "EZ",
        // ["Falkland Islands"] = "FK",
        // ["Faroe Islands"] = "FO",
        // ["Fiji"] = "FJ",
        // ["Finland"] = "FI",
        
        // ["French Guiana"] = "GF",
        // ["French Polynesia"] = "PF",
        // ["Gabon"] = "GA",
        // ["Gambia"] = "GM",
        // ["Georgia"] = "GE",
        
        // ["Ghana"] = "GH",
        // ["Gibraltar"] = "GI",
        // ["Greece"] = "GR",
        // ["Greenland"] = "GL",
        // ["Grenada"] = "GD",
        // ["Guadeloupe"] = "GP",
        // ["Guam"] = "GU",
        // ["Guatemala"] = "GT",
        // ["Guernsey"] = "GG",
        // ["Guinea"] = "GN",
        // ["Guinea-Bissau"] = "GW",
        // ["Guyana"] = "GY",
        // ["Haiti"] = "HT",
        // ["Holy See"] = "VA",
        // ["Honduras"] = "HN",
        // ["Hong Kong"] = "HK",
        // ["Hungary"] = "HU",
        // ["Iceland"] = "IS",
        // ["India"] = "IN",
        // ["Indonesia"] = "ID",
        // ["Iran"] = "IR",
        // ["Iraq"] = "IQ",
        // ["Isle of Man"] = "IM",
        // ["Israel"] = "IL",
        // ["Italy"] = "IT",
        // ["Jamaica"] = "JM",        
        // ["Jordan"] = "JO",
        // ["Kazakhstan"] = "KZ",
        // ["Kenya"] = "KE",
        // ["Kiribati"] = "KI",
        // ["Kuwait"] = "KW",
        // ["Kyrgyzstan"] = "KG",
        // ["Latvia"] = "LV",
        // ["Lebanon"] = "LB",
        // ["Lesotho"] = "LS",
        // ["Liberia"] = "LR",
        // ["Libya"] = "LY",
        // ["Liechtenstein"] = "LI",
        // ["Lithuania"] = "LT",
        // ["Luxembourg"] = "LU",
        // ["Macao"] = "MO",
        // ["Madagascar"] = "MG",
        // ["Malawi"] = "MW",
        // ["Malaysia"] = "MY",
        // ["Maldives"] = "MV",
        // ["Mali"] = "ML",
        // ["Malta"] = "MT",
        // ["Marshall Islands"] = "MH",
        // ["Martinique"] = "MQ",
        // ["Mauritania"] = "MR",
        // ["Mauritius"] = "MU",
        // ["Mayotte"] = "YT",
        // ["Mexico"] = "MX",
        // ["Moldova"] = "MD",
        // ["Monaco"] = "MC",
        // ["Mongolia"] = "MN",
        // ["Montenegro"] = "ME",
        // ["Montserrat"] = "MS",
        // ["Morocco"] = "MA",
        // ["Mozambique"] = "MZ",
        // ["Myanmar"] = "MM",
        // ["Namibia"] = "NA",
        // ["Nauru"] = "NR",
        // ["Nepal"] = "NP",
        // ["New Caledonia"] = "NC",
        // ["New Zealand"] = "NZ",
        // ["Nicaragua"] = "NI",
        // ["Niger"] = "NE",
        // ["Nigeria"] = "NG",
        // ["Niue"] = "NU",
        // ["Norfolk Island"] = "NF",
        // ["North Macedonia"] = "MK",
        // ["Northern Mariana Islands"] = "MP",
        // ["Norway"] = "NO",
        // ["Oman"] = "OM",
        // ["Pakistan"] = "PK",
        // ["Palau"] = "PW",
        // ["Palestine"] = "PS",
        // ["Panama"] = "PA",
        // ["Papua New Guinea"] = "PG",
        // ["Paraguay"] = "PY",
        // ["Peru"] = "PE",
        // ["Philippines"] = "PH",
        // ["Pitcairn"] = "PN",
        // ["Poland"] = "PL",
        // ["Portugal"] = "PT",
        // ["Puerto Rico"] = "PR",
        // ["Qatar"] = "QA",
        // ["Romania"] = "RO",
        // ["Russia"] = "RU",
        // ["Rwanda"] = "RW",
        // ["Saint Helena"] = "SH",
        // ["Saint Lucia"] = "LC",
        // ["Saint Martin"] = "MF",
        // ["Saint Pierre and Miquelon"] = "PM",
        // ["Saint Vincent and the Grenadines"] = "VC",
        // ["Samoa"] = "WS",
        // ["San Marino"] = "SM",
        // ["Sao Tome and Principe"] = "ST",
        // ["Saudi Arabia"] = "SA",
        // ["Senegal"] = "SN",
        // ["Serbia"] = "RS",
        // ["Seychelles"] = "SC",
        // ["Sierra Leone"] = "SL",
        // ["Singapore"] = "SG",
        // ["Slovakia"] = "SK",
        // ["Slovenia"] = "SI",
        // ["Solomon Islands"] = "SB",
        // ["Somalia"] = "SO",
        // ["South Africa"] = "ZA",
        // ["South Sudan"] = "SS",
        // ["South-Korea"] = "KR",
        // ["Sri Lanka"] = "LK",
        // ["Sudan"] = "SD",
        // ["Suriname"] = "SR",        
        // ["Syrian Arab Republic"] = "SY",
        // ["Taiwan"] = "TW",
        // ["Tajikistan"] = "TJ",
        // ["Tanzania"] = "TZ",
        // ["Thailand"] = "TH",
        // ["Timor-Leste"] = "TL",
        // ["Togo"] = "TG",
        // ["Tokelau"] = "TK",
        // ["Tonga"] = "TO",
        // ["Trinidad and Tobago"] = "TT",
        // ["Tunisia"] = "TN",
        // ["Turkey"] = "TR",
        // ["Turkmenistan"] = "TM",
        // ["Turks and Caicos Islands"] = "TC",
        // ["Tuvalu"] = "TV",
        // ["UAE"] = "AE",
        // ["Uganda"] = "UG",
        // ["Ukraine"] = "UA",
        // ["Uruguay"] = "UY",
        // ["Uzbekistan"] = "UZ",
        // ["Vanuatu"] = "VU",
        // ["Venezuela"] = "VE",
        // ["Viet Nam"] = "VN",
        // ["Virgin Islands"] = "VG",
        // ["Western Sahara"] = "EH",
        // ["Yemen"] = "YE",
        // ["Zambia"] = "ZM",
        // ["Zimbabwe"] = "ZW",
    };
    if (!fromCountryNameToAlpha2Dictionary.ContainsKey(countryName))
        throw new Exception("Please add the following country label to the country mapping list in the macro: " + countryName);
    return fromCountryNameToAlpha2Dictionary[countryName];
}