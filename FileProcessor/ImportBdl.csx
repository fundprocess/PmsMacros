#region Setup file definition
class BdlSecPosNode // /response/response/cont/pos/Secpos
{
    public string FileName { get; set; }
    public string ContId { get; set; } // /response/response/cont/@ContId
    public string SecurityCode { get; set; } // <SecurityCode>24493476</SecurityCode>
    public string Isin { get; set; }//<Isin>LU0947895834</Isin>
    public double AssetQty { get; set; } // <AssetQty>2190</AssetQty>
    public DateTime AssBalDate { get; set; } // <AssBalDate>2018-07-04</AssBalDate>
    public double TotSec { get; set; } // <TotSec>171696.00</TotSec>
    public double TotRefCcy { get; set; } // <TotRefCcy>171696.00</TotRefCcy>
    public string PosId { get; set; }
}
class BdlSecBaseNode // <Secbase>
{
    public string SecurityCode { get; set; } // <SecurityCode>13555251</SecurityCode>
    public string Isin { get; set; } // <Isin>FR0010400762</Isin>
    public string Telekurs { get; set; } // <Telekurs>2838572</Telekurs>
    public string Reuters { get; set; } // <Reuters/>
    public string Bloomberg { get; set; } // <Bloomberg/>
    public string Wkn { get; set; } // <Wkn>A1CV0R</Wkn>
    public string SecName { get; set; } // <SecName>Moneta Asset Management Moneta Long Short - A CAP</SecName>
    public int InstrType { get; set; } // <InstrType>880</InstrType>
    public string Domicile { get; set; } // <Domicile>FR</Domicile>
    public string InstrCcy { get; set; } // <InstrCcy>EUR</InstrCcy>
    public string ValFreq { get; set; } // <ValFreq>7310</ValFreq> 7310=daily, 7312=weekly
    public string MifidRisk { get; set; } // <MifidRisk>Risk Level 4 (04)</MifidRisk>
    public string EurAmStyle { get; set; }   // <EurAmStyle>AMERICAN</EurAmStyle>
    public string UnderlAsset { get; set; }   // <UnderlAsset>US4642876555</UnderlAsset>
    public double? ContrSize { get; set; }   // <ContrSize>100</ContrSize>
    public double? StrikePrice { get; set; }   // <StrikePrice>145</StrikePrice>
    public string EconSector { get; set; }   // <EconSector>84</EconSector>
    public string Issuer {get;set;} //<Issuer>Petroleos Mexicanos PEMEX</Issuer>
    public double? IntrRate {get;set;} // <IntrRate>5.5</IntrRate>
    public double? YieldToMat{get;set;} //<YieldToMat>0.081287</YieldToMat>
    public double? FaceAmt{get;set;} //<FaceAmt>1000</FaceAmt>
    public DateTime? MatRdmptDate{get;set;} //<MatRdmptDate>2044-06-27</MatRdmptDate>
    public DateTime? NextCoupDate{get;set;} //<NextCoupDate>2020-06-27</NextCoupDate>
}
class BdlCustidNode
{
    public string ContId { get; set; } // <cont ContId="02812601.1001">
    public string Domicile { get; set; } // <Domicile>MC</Domicile>
    public string DefaultCcy { get; set; } // <DefaultCcy>EUR</DefaultCcy>
}
class BdlCashposNode // /response/response/cont/pos/Cashpos
{
    public string ContId { get; set; } // /response/response/cont/@ContId
    public string Iban { get; set; } // <Iban>LU200080281260102005</Iban>
    public string AssetCcy { get; set; } // <AssetCcy>USD</AssetCcy>
    public DateTime PosBalDate { get; set; } // <PosBalDate>2018-07-04</PosBalDate>
    public double PosBalRefCcy { get; set; } // <PosBalRefCcy>20.37</PosBalRefCcy>
    public double AccrInt { get; set; } // <AccrInt>0.00</AccrInt>
    public string PosId { get; internal set; }
    public string FileName { get; set; }
}
class BdlFileDefinition : XmlFileDefinition
{
    public BdlFileDefinition()
    {
        this.AddNodeDefinition(XmlNodeDefinition.Create("secbase", "/response/response/cont/pos/Secbase", i => new BdlSecBaseNode
        {
            Bloomberg = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Bloomberg"),
            Domicile = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Domicile"),
            InstrCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/InstrCcy"),
            InstrType = i.ToXPathQuery<int>("/response/response/cont/pos/Secbase/InstrType"),
            Isin = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Isin"),
            MifidRisk = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/MifidRisk"),
            Reuters = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Reuters"),
            SecName = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/SecName"),
            SecurityCode = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/SecurityCode"),
            Telekurs = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Telekurs"),
            ValFreq = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/ValFreq"),
            EurAmStyle = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/EurAmStyle"),
            UnderlAsset = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/UnderlAsset"),
            Wkn = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Wkn"),
            ContrSize = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/ContrSize"),
            StrikePrice = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/StrikePrice"),
            EconSector = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/EconSector"),
            Issuer = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Issuer"),
            IntrRate = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/IntrRate"),
            YieldToMat = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/YieldToMat"),
            FaceAmt = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/FaceAmt"),
            MatRdmptDate = i.ToXPathQuery<DateTime?>("/response/response/cont/pos/Secbase/MatRdmptDate"),
            NextCoupDate = i.ToXPathQuery<DateTime?>("/response/response/cont/pos/Secbase/NextCoupDate")
        }));
        this.AddNodeDefinition(XmlNodeDefinition.Create("secpos", "/response/response/cont/pos/Secpos", i => new BdlSecPosNode
        {
            PosId = i.ToXPathQuery<string>("/response/response/cont/pos/@PosId"),
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            AssBalDate = i.ToXPathQuery<DateTime>("/response/response/cont/pos/Secpos/AssBalDate"),
            AssetQty = i.ToXPathQuery<double>("/response/response/cont/pos/Secpos/AssetQty"),
            SecurityCode = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/SecurityCode"),
            Isin  = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/Isin"),
            TotRefCcy = i.ToXPathQuery<double>("/response/response/cont/pos/Secpos/TotRefCcy"),
            TotSec = i.ToXPathQuery<double>("/response/response/cont/pos/Secpos/TotSec"),
            FileName = i.ToSourceName()
        }));
        this.AddNodeDefinition(XmlNodeDefinition.Create("cashpos", "/response/response/cont/pos/Cashpos", i => new BdlCashposNode
        {
            PosId = i.ToXPathQuery<string>("/response/response/cont/pos/@PosId"),
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            AccrInt = i.ToXPathQuery<double>("/response/response/cont/pos/Cashpos/AccrInt"),
            AssetCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Cashpos/AssetCcy"),
            Iban = i.ToXPathQuery<string>("/response/response/cont/pos/Cashpos/Iban"),
            PosBalDate = i.ToXPathQuery<DateTime>("/response/response/cont/pos/Cashpos/PosBalDate"),
            PosBalRefCcy = i.ToXPathQuery<double>("/response/response/cont/pos/Cashpos/PosBalRefCcy"),
            FileName = i.ToSourceName()
        }));
        this.AddNodeDefinition(XmlNodeDefinition.Create("custid", "/response/response/cont/Custid", i => new BdlCustidNode
        {
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            DefaultCcy = i.ToXPathQuery<string>("/response/response/cont/Custid/DefaultCcy"),
            Domicile = i.ToXPathQuery<string>("/response/response/cont/Custid/Domicile")
        }));
    }
}
#endregion

#region get sources

var nodesStream = FileStream
    .CrossApplyXmlFile($"{TaskName}: parse input file", new BdlFileDefinition());

var fileTargetSecuritiesStream = nodesStream
    .XmlNodeOfType<BdlSecBaseNode>($"{TaskName}: list only target securities")
    .SetForCorrelation($"{TaskName}: correlate target securities")
    .Distinct($"{TaskName}: distinct target securities", i => i.SecurityCode);

var fileSecurityPositionsStream = nodesStream
    .XmlNodeOfType<BdlSecPosNode>($"{TaskName}: list only securities positions")
    .SetForCorrelation($"{TaskName}: correlate securities positions")
    .Distinct($"{TaskName}: sum security positions duplicates within a file",
        i => new { i.FileName, i.AssBalDate, i.ContId, i.SecurityCode },
        o => o
            .ForProperty(a => a.AssetQty, DistinctAggregator.Sum)
            .ForProperty(a => a.TotSec, DistinctAggregator.Sum)
            .ForProperty(a => a.TotRefCcy, DistinctAggregator.Sum))
    .Distinct($"{TaskName}: exclude security positions duplicates", i => new { i.AssBalDate, i.ContId, i.SecurityCode });

var fileCashPositionsStream = nodesStream
    .XmlNodeOfType<BdlCashposNode>($"{TaskName}: list only cash positions")
    .SetForCorrelation($"{TaskName}: correlate cash positions")
    .Distinct($"{TaskName}: sum cash positions duplicates within a file",
        i => new { i.FileName, i.PosBalDate, i.ContId, i.Iban },
        o => o
            .ForProperty(a => a.PosBalRefCcy, DistinctAggregator.Sum)
            .ForProperty(a => a.AccrInt, DistinctAggregator.Sum))
    .Distinct($"{TaskName}: exclude cash positions duplicates", i => new { i.PosBalDate, i.ContId, i.Iban })
    .Fix($"{TaskName}: correct cash fields", o => o.FixProperty(i => i.Iban).IfNullWith(i => $"BDL_{i.ContId}_{i.AssetCcy}"));

var filePortfoliosStream = nodesStream
    .XmlNodeOfType<BdlCustidNode>($"{TaskName}: list only portfolios")
    .SetForCorrelation($"{TaskName}: correlate portfolios")
    .Distinct($"{TaskName}: distinct portfolio", i => i.ContId);

#endregion

var portfolioStream = filePortfoliosStream
    .LookupCurrency($"{TaskName}: get related currency for portfolio", l => l.DefaultCcy, (l, r) => new { l.ContId, CurrencyId = r?.Id })
    .Select($"{TaskName}: create portfolio", i => new DiscretionaryPortfolio
    {
        InternalCode = i.ContId,
        Name = $"BDL_{i.ContId}",
        ShortName = $"BDL_{i.ContId}".Truncate(MaxLengths.ShortName),
        CurrencyId = i.CurrencyId,
        // CountryCode = i.Domicile
    })
    .EfCoreSave($"{TaskName}: save portfolio", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var allPositions = fileSecurityPositionsStream.Union($"{TaskName}: merge all cash and security positions", fileCashPositionsStream,
        (l, r) => new { Date = l.AssBalDate, SecurityInternalCode = l.ContId, SecurityPosition = l, CashPosition = r },
        (l, r) => new { Date = r.PosBalDate, SecurityInternalCode = r.ContId, SecurityPosition = l, CashPosition = r })
    .Lookup($"{TaskName}: get related portfolio", portfolioStream,
        i => i.SecurityInternalCode,
        i => i.InternalCode,
        (l, r) => new { l.Date, l.CashPosition, l.SecurityPosition, Portfolio = r });

var portfolioCompositionStream = allPositions
    .Distinct($"{TaskName}: distinct portfolio composition", i => new { i.Date, i.Portfolio.Id })
    .EfCoreSave($"{TaskName}: save composition", o => o
        .Entity(i => new PortfolioComposition
        {
            Date = i.Date,
            PortfolioId = i.Portfolio.Id
        })
        .SeekOn(i => new { i.Date, i.PortfolioId })
        .Output((input, savedEntity) => new
        {
            input.Date,
            input.Portfolio,
            PortfolioComposition = savedEntity
        }));

#region Cash

var targetCashStream = fileCashPositionsStream
    .Distinct($"{TaskName}: distinct target cash", i => i.Iban)
    .LookupCurrency($"{TaskName}: get related currency for target cash", l => l.AssetCcy, (l, r) => new { l.Iban, CurrencyId = r?.Id })
    .Select($"{TaskName}: create target cash", i => new Cash
    {
        InternalCode = i.Iban,
        Iban = i.Iban,
        CurrencyId = i.CurrencyId,
        Name = $"{i.Iban}",
        ShortName = $"BDL_{i.Iban}".Truncate(MaxLengths.ShortName),
    })
    .EfCoreSave($"{TaskName}: save target cash", o => o.SeekOn(i => i.Iban).DoNotUpdateIfExists());

var cashPositionToSaveStream = fileCashPositionsStream
    .Lookup($"{TaskName}: lookup target cash", targetCashStream, i => i.Iban, i => i.Iban, (l, r) => new { FromFile = l, Cash = r })
    .Lookup($"{TaskName}: lookup cash portfolio", portfolioStream, i => i.FromFile.ContId, i => i.InternalCode, (l, r) => new { l.FromFile, l.Cash, Portfolio = r })
    .CorrelateToSingle($"{TaskName}: lookup cash portfolio composition", portfolioCompositionStream, (l, r) => new { l.FromFile, l.Cash, l.Portfolio, r.PortfolioComposition })
    .Select($"{TaskName}: create cash pos", i => new Position
    {
        PortfolioCompositionId = i.PortfolioComposition.Id,
        SecurityId = i.Cash.Id,
        Value = 1,
        MarketValueInPortfolioCcy = i.FromFile.PosBalRefCcy + i.FromFile.AccrInt
    });

#endregion

#region Security

var targetSecurityInstrumentStream = fileTargetSecuritiesStream
    .Distinct($"{TaskName}: distinct target positions security", i => i.Isin ?? i.SecurityCode)
    .LookupCurrency($"{TaskName}: get related currency for target security", l => l.InstrCcy, (l, r) => new {Node =l,l.InstrType, l.Isin, l.SecurityCode, l.SecName, l.Domicile, l.ValFreq, l.EurAmStyle, l.UnderlAsset, l.StrikePrice, l.ContrSize, CurrencyId = r?.Id })
    .LookupCountry($"{TaskName}: get related country for target security", l => l.Domicile, (l, r) => new {Node =l.Node, l.InstrType, l.Isin, l.SecurityCode, l.SecName, l.Domicile, l.ValFreq, l.CurrencyId, l.EurAmStyle, l.UnderlAsset, l.StrikePrice, l.ContrSize, CountryId = r?.Id })
    .Select($"{TaskName}: create target security", i => CreateTargetSecurity(i.InstrType, i.Isin, i.SecurityCode, i.SecName, 
                                    i.Domicile, i.CurrencyId, i.ValFreq, i.CountryId, i.EurAmStyle, 
                                    i.UnderlAsset, i.StrikePrice, i.ContrSize,
                                    i.Node.FaceAmt,i.Node.IntrRate,i.Node.NextCoupDate,new DateTime(2020,10,01)))
    .WhereCorrelated($"{TaskName}: keep known security instrument types", i => i != null)
    .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var instrumentPositionsToSaveStream = fileSecurityPositionsStream
    .Lookup($"{TaskName}: lookup related target security by Isin", targetSecurityInstrumentStream, 
            i => (!string.IsNullOrEmpty(i.Isin))?i.Isin:i.SecurityCode, i => i.InternalCode, (l, r) => new { FromFile = l, Security1 = r })
    .Lookup($"{TaskName}: lookup related target security by Security Code", targetSecurityInstrumentStream, 
             i => i.FromFile.SecurityCode, i => i.InternalCode, (l, r) => new { FromFile = l.FromFile, Security1 = l.Security1, Security2 = r })
    .Lookup($"{TaskName}: lookup security portfolio", portfolioStream, i => i.FromFile.ContId, i => i.InternalCode, (l, r) => new { l.FromFile, l.Security1, l.Security2, Portfolio = r })
    .CorrelateToSingle($"{TaskName}: lookup security portfolio composition", portfolioCompositionStream, (l, r) => new { l.FromFile, l.Security1, l.Security2, l.Portfolio, r.PortfolioComposition })
    .Select($"{TaskName}: create security pos", i => new Position
    {
        PortfolioCompositionId = i.PortfolioComposition.Id,
        SecurityId = (i.Security1!=null)? i.Security1.Id: ((i.Security2!=null)?i.Security2.Id:throw new Exception($"Security not found {i.FromFile.SecurityCode}")),
        Value = i.FromFile.AssetQty,
        MarketValueInSecurityCcy = i.FromFile.TotSec,
        MarketValueInPortfolioCcy = i.FromFile.TotRefCcy
    });
#endregion
var savedPositions = cashPositionToSaveStream
    .Union($"{TaskName}: join to instrument positions", instrumentPositionsToSaveStream)
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));
return FileStream.WaitWhenDone($"{TaskName}: wait end of all save", savedPositions);

Security CreateTargetSecurity(int instrType, string isin, string securityCode, string name, 
                string domicile, int? currencyId, string valFreq, int? countryId, string eurAmStyle, 
                string underlAsset, double? strikePrice, double? contractSize,
                double? faceValue,double? rate, DateTime? nextCouponDate,DateTime? maturityDate)
{
    Security security = null;
    switch (instrType)
    {
        case 101:
        case 104:
            security = new Bond();
            break;
        case 201:
        case 202:
            security = new Equity();
            break;
        case int n when (n >= 310 && n <= 320):
            security = new Option();
            break;
        case int n when (n >= 800 && n < 900):
            security = new ShareClass();
            break;
        case int n when (n >= 900 && n < 1000):
            security = new Cash();
            break;
        default:
            throw new Exception($"Unknown instrument type {instrType} not managed: (isin: {isin}, name: {name})");
    }

    if (security == null) return null;
    security.CurrencyId = currencyId;
    security.InternalCode = securityCode; 
    security.Name = name;
    security.ShortName = name.Truncate(MaxLengths.ShortName);
    if (security is OptionFuture der)
    {
        der.UnderlyingIsin = isin;
    }
    if (security is SecurityInstrument securityInstrument)
    {
        securityInstrument.InternalCode = !string.IsNullOrEmpty(isin)?isin:securityCode;
        securityInstrument.Isin = isin;
    }
    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.CountryId = countryId;
    }
    if (security is Bond bond)
    {
        if (rate.HasValue)
            bond.CouponRate = rate.Value/100;
        if (faceValue.HasValue)
            bond.FaceValue = faceValue.Value;
        if (nextCouponDate.HasValue)
            bond.NextCouponDate = nextCouponDate.Value;
        if (maturityDate.HasValue)
            bond.MaturityDate = maturityDate.Value;
            
        // CouponType CouponType= ... ;
        // bool IsPerpetual= ... ;
        // DateTime? FirstPaymentDate= ... ;
        // DateTime? PreviousCouponDate= ... ;
        // FrequencyType? CouponFrequency= ... ;
        // BondIssuerType? IssuerType = ... ;
        // double? IssueAmount= ... ;
        // bool IsCallable= ... ;
        // DateTime? NextCallDate= ... ;
    }
    if (security is Option option)
    {
        option.Type = eurAmStyle == "AMERICAN" ? OptionType.American : OptionType.European;
        option.PutCall = name.Contains("put", System.StringComparison.InvariantCultureIgnoreCase) ? PutCall.Put : PutCall.Call;
    }
    if (security is StandardDerivative standardDerivative)
    {
        standardDerivative.StrikePrice = strikePrice;
        standardDerivative.ContractSize = contractSize;
    }
    
    return security;
}
