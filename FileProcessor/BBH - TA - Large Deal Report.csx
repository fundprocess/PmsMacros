var fileDefinition = FlatFileDefinition.Create(i => new
{
    TfcId = i.ToColumn("TfcId"),
    ClientName = i.ToColumn("Client Name"),
    DateFrom = i.ToOptionalDateColumn("Date From","dd-MMM-yy"),
    DateTo = i.ToOptionalDateColumn("Date To","dd-MMM-yy"),
    FundNumber = i.ToColumn("Fund Number"),
    FundName = i.ToColumn("Fund Name"),
    UnitType = i.ToColumn("Unit Type"),
    UnitTypeDescription = i.ToColumn("Unit Type Description"),
    TransactionDate = i.ToOptionalDateColumn("Transaction Date","dd-MMM-yy"),
    SettlementDate = i.ToOptionalDateColumn("Settlement Date","dd-MMM-yy"),
    TransactionType = i.ToColumn("Transaction Type"),
    FundCurrency = i.ToColumn("Fund Currency"),
    Shares = i.ToNumberColumn<double?>("Shares","."),
    Price = i.ToNumberColumn<double?>("Price","."),
    //FundCurrencyTASubFund = i.ToColumn("Fund Currency( TA Sub Fund)"),
    Value = i.ToNumberColumn<double?>("Value","."),
    OrderNumber = i.ToNumberColumn<double?>("Order Number","."),
    OrderStatus = i.ToNumberColumn<double?>("Order Status","."),
    OutletNumber = i.ToColumn("Outlet Number"),
    OutletName = i.ToColumn("Outlet Name"),
    RegisterNumber = i.ToColumn("Register Number"),
    RegisterName = i.ToColumn("Register Name"),
    RegisterFirstname = i.ToColumn("Register Firstname"),
    //RegisterShortname = i.ToColumn("Register Shortname"),
    ContractNumber = i.ToColumn("Contract Number"),
    //PercentageOfShareClass = i.ToColumn("% of Share Class"),
    ReportingCurrency = i.ToColumn("Reporting Currency"),
    TransBase = i.ToColumn("Trans Base"),
    RegisterDealNumber = i.ToColumn("Register Deal Number"),
    RegisterDealName = i.ToColumn("Register Deal Name"),
    RegisterDealFirstname = i.ToColumn("Register Deal Firstname"),
    SalesType = i.ToColumn("Sales Type"),
    BaseCurrency = i.ToColumn("Base Currency"),
    PaymentCurrency = i.ToColumn("Payment Currency"),
    ValueInClassCurrency = i.ToNumberColumn<double?>("Value in Class Currency","."),
    ISIN = i.ToColumn("ISIN"),
    HedgeAccount = i.ToColumn("Hedge Account"),
    FundHeadAccount = i.ToColumn("Fund Head Account"),
    Omegano = i.ToColumn("Omega no"),
    OperatingCurrency = i.ToColumn("Operating Currency"),
    ValueInBaseCurrency = i.ToNumberColumn<double?>("Value in Base Currency","."),
    //PercentageOfTotalFund = i.ToColumn("% of Total Fund"),
    CountryOfResidence = i.ToColumn("Country of Residence"),
    GAV = i.ToColumn("GAV"),
    Benchmark = i.ToColumn("Benchmark"),
    GrossCRP = i.ToColumn("Gross CRP"),
    RevisedCRP = i.ToColumn("Revised CRP"),
    Equalisation = i.ToColumn("Equalisation"),
    EqualisationLeft = i.ToColumn("EqualisationLeft"),
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));;

var subRedStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

#region Create Investors
string getClientCode(string registerNumber)
    => "BBH-"+ registerNumber;

var clientEntitiesStream = subRedStream
    .Distinct($"{TaskName} Distinct client" , i => getClientCode(i.RegisterNumber))
    .LookupCountry($"{TaskName}: get related country", i => GetCountryIsoFromBbhName(i.CountryOfResidence), 
        (l, r) => new {FileRow = l, Country = r})
    .LookupCurrency($"{TaskName}: get related currency", l => l.FileRow.PaymentCurrency, 
        (l, r) =>  new {l.FileRow, l.Country, InvestorCurrency = r})
    .Select($"{TaskName}: create company entity", i => new Company
    {
        InternalCode = !string.IsNullOrEmpty(i.FileRow.RegisterNumber)? getClientCode(i.FileRow.RegisterNumber)
                        : throw new Exception("Empty line error, please check you csv content"),
        Name = i.FileRow.RegisterFirstname+" "+i.FileRow.RegisterName,
        CurrencyId = i.InvestorCurrency != null? i.InvestorCurrency.Id : (int?)null,
        CountryId = i.Country != null? i.Country.Id: (int?)null,
        Culture = new CultureInfo("en"),
        RegistrationNumber = i.FileRow.OutletNumber,
        YearEnd = new DateOfYear(12,31)
    })
    .EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var investorRelationshipsStream = subRedStream
    .Distinct($"{TaskName} Distinct client 2" , i => getClientCode(i.RegisterNumber))
    .CorrelateToSingle($"{TaskName}: get related FileRow", clientEntitiesStream, 
        (l, r) => new { FileRow = l, Company = r})
    .Select($"{TaskName} create company Investor Relationship", i => new {
        EntityInternalCode = i.Company != null ? i.Company.InternalCode : throw new Exception("Underlying investor entity not found") , 
        Relationship= new InvestorRelationship{
            EntityId = i.Company.Id,
            StartDate = i.FileRow.TransactionDate,
            InvestorType = i.FileRow.RegisterFirstname == null ? InvestorType.Institutional
                            : i.FileRow.RegisterFirstname.ToLower().Contains("ltd")? InvestorType.Institutional
                            : i.FileRow.RegisterFirstname.ToLower().Contains(" sa")? InvestorType.Institutional
                            : InvestorType.Retail,
            StatementFrequency = FrequencyType.Quarterly,
            CurrencyId = (i.Company.CurrencyId.HasValue) ? i.Company.CurrencyId.Value : (int?) null,
        }
    })
    .EfCoreSave($"{TaskName}: Save Company Investor Relationship", o => o
        .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));

#endregion Create Investors


var portfoliosStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get portfolios", (ctx,j) => ctx.Set<Portfolio>());
var shareClassesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get shareClasses", (ctx,j) => ctx.Set<ShareClass>());
var securityTransactionsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get security transactions", (ctx,j) => ctx.Set<SecurityTransaction>().Include(i => i.Security));

#region SubRed => Security Transactions

string GetTransactionCode(string OutletNumber, double? OrderNumber, DateTime TransactionDate)
    =>  "BBH - SubRed - " + OutletNumber + " - " + OrderNumber + " - " + TransactionDate.ToString("yyyyMMdd");

var savedSecTransStream = subRedStream
    .Lookup($"{TaskName} SecTrans - Lookup related portfolio by name",portfoliosStream, i => i.FundName.ToLower(), i => i.Name.ToLower(),
        (l,r) => new {FileRow = l, Portfolio = r})
    .LookupCurrency($"{TaskName}: SecTrans - get currency", i => i.FileRow.BaseCurrency, 
        (l, r) => new { l.FileRow, l.Portfolio, Currency = r})
    .Lookup($"{TaskName} SecTrans - Lookup related share class", shareClassesStream, i => i.FileRow.ISIN, i => i.InternalCode,
        (l, r) => new { l.FileRow, l.Portfolio, l.Currency, ShareClass = r })
    .Select($"{TaskName}: Create SecTrans", i => new SecurityTransaction {
        PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found for: " + i.FileRow.FundName),
        TransactionCode = GetTransactionCode(i.FileRow.OutletNumber, i.FileRow.OrderNumber, i.FileRow.TransactionDate.Value),
        TransactionType = TransactionType.SubscriptionRedemption,

        TradeDate =  (i.FileRow.TransactionDate != null)? i.FileRow.TransactionDate.Value : throw new Exception("Transaction date not provided"),
        NavDate =  (i.FileRow.TransactionDate != null)? i.FileRow.TransactionDate.Value : throw new Exception("Transaction date not provided"),
        ValueDate = (i.FileRow.SettlementDate != null)? i.FileRow.SettlementDate.Value : throw new Exception("SettlementDate date not provided"),
        SettlementDate = (i.FileRow.SettlementDate != null)? i.FileRow.SettlementDate.Value : throw new Exception("SettlementDate date not provided"),

        Description = i.FileRow.RegisterFirstname +" "+ i.FileRow.RegisterName,
        
        GrossAmountInSecurityCcy = i.FileRow.ValueInClassCurrency.Value,
        GrossAmountInPortfolioCcy = i.FileRow.ValueInBaseCurrency.Value,
        NetAmountInSecurityCcy = i.FileRow.ValueInClassCurrency.Value,
        NetAmountInPortfolioCcy = i.FileRow.ValueInBaseCurrency.Value, 
        PriceInSecurityCcy = i.FileRow.Price.Value,
        Quantity = i.FileRow.Shares.Value,
        SecurityId = (i.ShareClass != null)? i.ShareClass.Id: throw new Exception("Share Class not found in DB: " + i.FileRow.ISIN),
        // int? BrokerId = ,
        // MultiCultureString Comment = ,

        DecisionType = TransactionDecisionType.Discretionary,        
        FeesInSecurityCcy = 0,
        OperationType = i.FileRow.TransactionType.ToLower() == "subscription"? OperationType.Buy
                        :(i.FileRow.TransactionType.ToLower() == "redemption"? OperationType.Sale
                        :throw new Exception("Unknown TransactionType :" + i.FileRow.TransactionType)),
    })
    .EfCoreSave($"{TaskName}: Save SecTrans", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());

#endregion

#region SubRed => Cash Movements
var savedCashMovementsStream = subRedStream
    .Lookup($"{TaskName} Lookup related portfolio by name",portfoliosStream, i => i.FundName.ToLower(), i => i.Name.ToLower(),
        (l,r) => new {FileRow = l, Portfolio = r})
    .LookupCurrency($"{TaskName}: get currency", i => i.FileRow.BaseCurrency, 
        (l, r) => new { l.FileRow, l.Portfolio, Currency = r})
    .Lookup($"{TaskName} Lookup related share class", shareClassesStream, i => i.FileRow.ISIN, i => i.InternalCode,
        (l, r) => new { l.FileRow, l.Portfolio, l.Currency, ShareClass = r })
    .CorrelateToSingle($"{TaskName} get related transaction", savedSecTransStream,
        (l, r) => new { l.FileRow, l.Portfolio, l.Currency, l.ShareClass, SecTrans = r })
    .Select($"{TaskName}: Create cash movement", i => new CashMovement{
        PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found for: " + i.FileRow.FundName),
        CurrencyId = (i.Currency != null)? i.Currency.Id : throw new Exception("Currency not found for: " + i.FileRow.BaseCurrency),
        MovementCode = GetTransactionCode(i.FileRow.OutletNumber, i.FileRow.OrderNumber, i.FileRow.TransactionDate.Value),
        TransactionCode = GetTransactionCode(i.FileRow.OutletNumber, i.FileRow.OrderNumber, i.FileRow.TransactionDate.Value),
        TransactionType = TransactionType.SubscriptionRedemption,

        TradeDate =  (i.FileRow.TransactionDate != null)? i.FileRow.TransactionDate.Value : throw new Exception("Transaction date not provided"),
        ValueDate = (i.FileRow.SettlementDate != null)? i.FileRow.SettlementDate.Value : throw new Exception("SettlementDate date not provided"),
        ClosingDate = (i.FileRow.SettlementDate != null)? i.FileRow.SettlementDate.Value : throw new Exception("SettlementDate date not provided"),
        
        Description = i.FileRow.RegisterFirstname +" "+ i.FileRow.RegisterName,
        ExternalTransactionCode = i.FileRow.TransactionType,
        
        GrossAmountInSecurityCcy = i.FileRow.ValueInClassCurrency.Value,
        GrossAmountInPortfolioCcy = i.FileRow.ValueInBaseCurrency.Value,
        NetAmountInSecurityCcy = i.FileRow.ValueInClassCurrency.Value,
        NetAmountInPortfolioCcy = i.FileRow.ValueInBaseCurrency.Value, 
        PriceInSecurityCcy = i.FileRow.Price.Value,
        Quantity = i.FileRow.Shares.Value,
        Reversal = false,
        UnderlyingSecurityId = (i.ShareClass != null)? i.ShareClass.Id: throw new Exception("Share Class not found in DB: " + i.FileRow.ISIN),
        TransactionId = (i.SecTrans != null)? i.SecTrans.Id
            : throw new Exception("Related SecTrans not found : " + GetTransactionCode(i.FileRow.OutletNumber, i.FileRow.OrderNumber, i.FileRow.TransactionDate.Value)),
        TaxesInSecurityCcy = 0,
        FeesInSecurityCcy = 0,
        BrokerageFeesInSecurityCcy = 0,
        // CounterpartyId = i.Broker.Id,
        // // int? CashId
    })
    .EfCoreSave($"{TaskName}: Save cash movement", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());
#endregion

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", investorRelationshipsStream, savedCashMovementsStream);

#region helpers
string GetCountryIsoFromBbhName(string countryLabel)
{
    if (countryLabel == null)
        throw new Exception("Country is null");

    countryLabel = countryLabel.ToLower();
    if (countryLabel == "Other")
        return string.Empty; 
    Dictionary<string, string> fromCountryNameToAlpha2Dictionary = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
    {
        ["united kingdom"] = "GB",
        ["great-britain"] = "GB",
        ["united states"] = "US",
        ["usa"] = "US",
        ["luxembourg"] = "LU",
        ["isle of man"] = "IM",
        ["man (isle of)"] = "IM",
        ["belgium"] = "BE",
        ["virgin islands"] = "VG",
        ["virgin islands (uk)"] = "VG",

        ["afghanistan"] = "AF",
        ["albania"] = "AL",
        ["algeria"] = "DZ",
        ["american samoa"] = "AS",
        ["andorra"] = "AD",
        ["angola"] = "AO",
        ["anguilla"] = "AI",
        ["antarctica"] = "AQ",
        ["antigua and barbuda"] = "AG",
        ["argentina"] = "AR",
        ["armenia"] = "AM",
        ["aruba"] = "AW",
        ["ascension island"] = "AC",
        ["australia"] = "AU",
        ["austria"] = "AT",
        ["azerbaijan"] = "AZ",
        ["bahamas"] = "BS",
        ["bahrain"] = "BH",
        ["bangladesh"] = "BD",
        ["barbados"] = "BB",
        ["belarus"] = "BY",
        ["belize"] = "BZ",
        ["benin"] = "BJ",
        ["bermuda"] = "BM",
        ["bhutan"] = "BT",
        ["bolivia"] = "BO",
        ["bosnia and herzegovina"] = "BA",
        ["botswana"] = "BW",
        ["bouvet island"] = "BV",
        ["brazil"] = "BR",
        ["british indian ocean territory"] = "IO",
        ["brunei darussalam"] = "BN",
        ["bulgaria"] = "BG",
        ["burkina faso"] = "BF",
        ["burundi"] = "BI",
        ["cabo verde"] = "CV",
        ["cambodia"] = "KH",
        ["cameroon"] = "CM",
        ["canada"] = "CA",
        ["canary islands"] = "IC",
        ["cayman islands"] = "KY",
        ["central african republic"] = "CF",
        ["chad"] = "TD",
        ["chile"] = "CL",
        ["china"] = "CN",
        ["christmas island"] = "CX",
        ["clipperton island"] = "CP",
        ["colombia"] = "CO",
        ["comoros"] = "KM",
        ["congo"] = "CG",
        ["cook islands"] = "CK",
        ["costa rica"] = "CR",
        ["cote d'ivoire"] = "CI",
        ["croatia"] = "HR",
        ["cuba"] = "CU",
        ["curaçao"] = "CW",
        ["cyprus"] = "CY",
        ["czechia"] = "CZ",
        ["denmark"] = "DK",
        ["diego garcia"] = "DG",
        ["djibouti"] = "DJ",
        ["dominica"] = "DM",
        ["dominican republic"] = "DO",
        ["ecuador"] = "EC",
        ["egypt"] = "EG",
        ["el salvador"] = "SV",
        ["equatorial guinea"] = "GQ",
        ["eritrea"] = "ER",
        ["estonia"] = "EE",
        ["eswatini"] = "SZ",
        ["ethiopia"] = "ET",
        ["european union"] = "EU",
        ["eurozone"] = "EZ",
        ["falkland islands"] = "FK",
        ["faroe islands"] = "FO",
        ["fiji"] = "FJ",
        ["finland"] = "FI",
        ["france"] = "FR",
        ["french guiana"] = "GF",
        ["french polynesia"] = "PF",
        ["gabon"] = "GA",
        ["gambia"] = "GM",
        ["georgia"] = "GE",
        ["germany"] = "DE",
        ["ghana"] = "GH",
        ["gibraltar"] = "GI",
        ["greece"] = "GR",
        ["greenland"] = "GL",
        ["grenada"] = "GD",
        ["guadeloupe"] = "GP",
        ["guam"] = "GU",
        ["guatemala"] = "GT",
        ["guernsey"] = "GG",
        ["guinea"] = "GN",
        ["guinea-bissau"] = "GW",
        ["guyana"] = "GY",
        ["haiti"] = "HT",
        ["holy see"] = "VA",
        ["honduras"] = "HN",
        ["hong kong"] = "HK",
        ["hungary"] = "HU",
        ["iceland"] = "IS",
        ["india"] = "IN",
        ["indonesia"] = "ID",
        ["iran"] = "IR",
        ["iraq"] = "IQ",
        ["ireland"] = "IE",
        ["israel"] = "IL",
        ["italy"] = "IT",
        ["jamaica"] = "JM",
        ["japan"] = "JP",
        ["jersey"] = "JE",
        ["jordan"] = "JO",
        ["kazakhstan"] = "KZ",
        ["kenya"] = "KE",
        ["kiribati"] = "KI",
        ["kuwait"] = "KW",
        ["kyrgyzstan"] = "KG",
        ["latvia"] = "LV",
        ["lebanon"] = "LB",
        ["lesotho"] = "LS",
        ["liberia"] = "LR",
        ["libya"] = "LY",
        ["liechtenstein"] = "LI",
        ["lithuania"] = "LT",
        ["macao"] = "MO",
        ["madagascar"] = "MG",
        ["malawi"] = "MW",
        ["malaysia"] = "MY",
        ["maldives"] = "MV",
        ["mali"] = "ML",
        ["malta"] = "MT",
        ["marshall islands"] = "MH",
        ["martinique"] = "MQ",
        ["mauritania"] = "MR",
        ["mauritius"] = "MU",
        ["mayotte"] = "YT",
        ["mexico"] = "MX",
        ["moldova"] = "MD",
        ["monaco"] = "MC",
        ["mongolia"] = "MN",
        ["montenegro"] = "ME",
        ["montserrat"] = "MS",
        ["morocco"] = "MA",
        ["mozambique"] = "MZ",
        ["myanmar"] = "MM",
        ["namibia"] = "NA",
        ["nauru"] = "NR",
        ["nepal"] = "NP",
        ["netherlands"] = "NL",
        ["new caledonia"] = "NC",
        ["new zealand"] = "NZ",
        ["nicaragua"] = "NI",
        ["niger"] = "NE",
        ["nigeria"] = "NG",
        ["niue"] = "NU",
        ["norfolk island"] = "NF",
        ["north macedonia"] = "MK",
        ["northern mariana islands"] = "MP",
        ["norway"] = "NO",
        ["oman"] = "OM",
        ["pakistan"] = "PK",
        ["palau"] = "PW",
        ["palestine"] = "PS",
        ["panama"] = "PA",
        ["papua new guinea"] = "PG",
        ["paraguay"] = "PY",
        ["peru"] = "PE",
        ["philippines"] = "PH",
        ["pitcairn"] = "PN",
        ["poland"] = "PL",
        ["portugal"] = "PT",
        ["puerto rico"] = "PR",
        ["qatar"] = "QA",
        ["romania"] = "RO",
        ["russia"] = "RU",
        ["rwanda"] = "RW",
        ["saint helena"] = "SH",
        ["saint lucia"] = "LC",
        ["saint martin"] = "MF",
        ["saint pierre and miquelon"] = "PM",
        ["saint vincent and the grenadines"] = "VC",
        ["samoa"] = "WS",
        ["san marino"] = "SM",
        ["sao tome and principe"] = "ST",
        ["saudi arabia"] = "SA",
        ["senegal"] = "SN",
        ["serbia"] = "RS",
        ["seychelles"] = "SC",
        ["sierra leone"] = "SL",
        ["singapore"] = "SG",
        ["slovakia"] = "SK",
        ["slovenia"] = "SI",
        ["solomon islands"] = "SB",
        ["somalia"] = "SO",
        ["south africa"] = "ZA",
        ["south sudan"] = "SS",
        ["south-korea"] = "KR",
        ["spain"] = "ES",
        ["sri lanka"] = "LK",
        ["sudan"] = "SD",
        ["suriname"] = "SR",
        ["sweden"] = "SE",
        ["switzerland"] = "CH",
        ["syrian arab republic"] = "SY",
        ["taiwan"] = "TW",
        ["tajikistan"] = "TJ",
        ["tanzania"] = "TZ",
        ["thailand"] = "TH",
        ["timor-leste"] = "TL",
        ["togo"] = "TG",
        ["tokelau"] = "TK",
        ["tonga"] = "TO",
        ["trinidad and tobago"] = "TT",
        ["tunisia"] = "TN",
        ["turkey"] = "TR",
        ["turkmenistan"] = "TM",
        ["turks and caicos islands"] = "TC",
        ["tuvalu"] = "TV",
        ["uae"] = "AE",
        ["uganda"] = "UG",
        ["ukraine"] = "UA",
        ["uruguay"] = "UY",
        ["uzbekistan"] = "UZ",
        ["vanuatu"] = "VU",
        ["venezuela"] = "VE",
        ["viet nam"] = "VN",
        ["western sahara"] = "EH",
        ["yemen"] = "YE",
        ["zambia"] = "ZM",
        ["zimbabwe"] = "ZW",
    };
    if (!fromCountryNameToAlpha2Dictionary.ContainsKey(countryLabel))
        throw new Exception("Country not recognized () in BBH - TA - Large Deal Report, please add it in the macro: " + countryLabel);
    return fromCountryNameToAlpha2Dictionary[countryLabel];
}

#endregion