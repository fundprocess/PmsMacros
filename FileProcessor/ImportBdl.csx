#region Setup file definition
class BdlCustidNode
{
    public string ContId { get; set; } // <cont ContId="02812601.1001">
    public string CollectionId{get;set;} //<CollectionId>53440040.3</CollectionId>
    public string Domicile { get; set; } // <Domicile>MC</Domicile>
    public string DefaultCcy { get; set; } // <DefaultCcy>EUR</DefaultCcy>
    public DateTime OpenDate {get;set;} // <OpenDate>2013-04-04</OpenDate>
    public string TaxDomicile {get;set;} //<TaxDomicile>MC</TaxDomicile>
    public int? EusdStatus {get;set;} //<EusdStatus>2</EusdStatus>
    public string InitMifid {get;set;} //<InitMifid>Privé (01)</InitMifid>
    public int? FinKnowl {get;set;} //<FinKnowl>5</FinKnowl>
    public int? FinExp {get;set;} //<FinExp>5</FinExp>
}
class BdlSecPosNode // /response/response/cont/pos/Secpos
{
    public string FileName { get; set; }
    public string ContId { get; set; } // /response/response/cont/@ContId
    public string PosId { get; set; }
    public string SecurityCode { get; set; } // <SecurityCode>24493476</SecurityCode>
    public string Isin { get; set; }//<Isin>LU0947895834</Isin>
    public double AssetQty { get; set; } // <AssetQty>2190</AssetQty>
    public DateTime AssBalDate { get; set; } // <AssBalDate>2018-07-04</AssBalDate>
    public string PriceCcy {get;set;}
    public double? UnitPrice { get; set; }
    public DateTime PriceDate { get; set; }
    public double? CostPrice { get; set; } //<CostPrice>599.1572166</CostPrice>
    public string aveCostCcy { get; set; } //<aveCostCcy>EUR</aveCostCcy>
    public double? UnReEur {get;set;} //<UnReEur>239634.23</UnReEur>
    public double TotSec { get; set; } // <TotSec>171696.00</TotSec>
    public string TotSecCcy {get;set; }//<TotSecCcy>EUR</TotSecCcy>
    public double TotRefCcy { get; set; } // <TotRefCcy>171696.00</TotRefCcy>
    public double? TotInClientCcy { get; set; } //<TotInClientCcy>1064074.56</TotInClientCcy>
    public double? AccrIntRef { get; set; } //<AccrIntRef>0</AccrIntRef>
    public string ClientCcy {get;set; } //<ClientCcy>EUR</ClientCcy>
    public string RefCcy {get;set; } //<RefCcy>EUR</RefCcy>
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
    public string MifidPTyp { get; set; } //<MifidPTyp>Equities</MifidPTyp>  
    public string CaaClassif  { get; set; }  
    public string CFIclass { get; set; } //<CFIclass>ESVUFN</CFIclass>
    public int? InstrType { get; set; } // <InstrType>880</InstrType>
    public int? InstrTypeL1 { get; set; } // <InstrTypeL1>15218</InstrTypeL1>
    public int? InstrTypeL2 { get; set; } // <InstrTypeL2>15219</InstrTypeL2>
    public string EconSector { get; set; } //<EconSector>95</EconSector>
    public string GeoSector { get; set; } //<GeoSector>01</GeoSector>
    public string WthldTax { get; set; } //<WthldTax>10012</WthldTax>
    public string TradingPlace { get; set; }
    public string EusdStatA { get; set; }
    public string Domicile { get; set; } // <Domicile>FR</Domicile>
    public string InstrCcy { get; set; } // <InstrCcy>EUR</InstrCcy>

    public string PriceUnit {get;set;}//<PriceUnit>mon</PriceUnit>
    public int QuotMode{get;set;} //<QuotMode>11</QuotMode>
    public string PriceCcy{get;set;} //<PriceCcy>EUR</PriceCcy>
    public double? Price{get;set;} // <Price>18.9</Price>
    public DateTime? PriceDate{get;set;}// <PriceDate>2020-11-17</PriceDate>

    public string Issuer{get;set;} // <Issuer>Compagnie des Alpes SA</Issuer>
    public string IssueDomic{get;set;} //<IssueDomic>FR</IssueDomic>

    public double? IntrRate {get;set;} // <IntrRate>5.5</IntrRate>
    public double? YieldToMat{get;set;} //<YieldToMat>0.081287</YieldToMat>
    public double? FaceAmt{get;set;} //<FaceAmt>1000</FaceAmt>
    public DateTime? MatRdmptDate{get;set;} //<MatRdmptDate>2044-06-27</MatRdmptDate>
    public DateTime? NextCoupDate{get;set;} //<NextCoupDate>2020-06-27</NextCoupDate>

    public int? ValFreq { get; set; } // <ValFreq>7310</ValFreq> 7310=daily, 7312=weekly
    public string MifidRisk { get; set; } // <MifidRisk>Risk Level 4 (04)</MifidRisk>
    public string MifidComplx{get;set;} //    <MifidComplx>Not complex (02)</MifidComplx>
    public string Mifid2Complx{get;set;} //    <Mifid2Complx>NON-COMPLEX PRODUCTS</Mifid2Complx>
   
    public string EurAmStyle { get; set; }   // <EurAmStyle>AMERICAN</EurAmStyle> or <EurAmStyle>EUROPEAN</EurAmStyle>
    public string UnderlAsset { get; set; }   // <UnderlAsset>US4642876555</UnderlAsset>
    public double? ContrSize { get; set; }   // <ContrSize>100</ContrSize>
    public double? StrikePrice { get; set; }   // <StrikePrice>145</StrikePrice>

    public int? FundValRdmpt{get;set;} //<FundValRdmpt>3</FundValRdmpt>
    public int? FundValSubs{get;set;}  //  <FundValSubs>3</FundValSubs>
    public double? MinSubsAmt{get;set;}  //<MinSubsAmt>5000</MinSubsAmt>
    public string TechCutoff{get;set;} //<TechCutoff>11:15</TechCutoff>
    public double? FrontEndLoad {get;set;} //<FrontEndLoad>.05</FrontEndLoad>
    public double? BackEndLoad {get;set;} 
}
class BdlCashposNode // /response/response/cont/pos/Cashpos
{
    public string ContId { get; set; } // /response/response/cont/@ContId
    public string PosId { get; internal set; }
    public string Iban { get; set; } // <Iban>LU200080281260102005</Iban>
    public string AssetCcy { get; set; } // <AssetCcy>USD</AssetCcy>
    public string ContractNr { get; set; } //<ContractNr>2700738</ContractNr>
    
    public DateTime PosBalDate { get; set; } // <PosBalDate>2018-07-04</PosBalDate>
    public double PosBalRefCcy { get; set; } // <PosBalRefCcy>20.37</PosBalRefCcy>
    
    public int AssetType {get;set;} // <AssetType>17063</AssetType>
    public double balBook { get; set; } // <balBook>7585.20</balBook>
    public double balVal { get; set; } // <balBook>7585.20</balBook>

    public double? AccrInt { get; set; } // <AccrInt>0.00</AccrInt>
    public string FileName { get; set; }
}

class BdlSectransNode 
{
    public string ContId { get; set; } // /response/response/cont/@ContId
    public string PosId { get; internal set; }
    public string ContIdSec {get;set;}// <ContIdSec>03998376.1001</ContIdSec>
    public string OrderNr {get;set;}// <OrderNr>283408849</OrderNr>
    public string SecurityCode {get;set;}// <SecurityCode>74112443</SecurityCode>
    public string Isin {get;set;}// <Isin>IE00BF5H5052</Isin>
    public string OrdTypId {get;set;}// <OrdTypId>140022</OrdTypId>
    public DateTime OrderDate {get;set;}// <OrderDate>2020-11-16</OrderDate>
    public DateTime ExecDate {get;set;}// <ExecDate>2020-11-16</ExecDate>
    public string TradingPlace {get;set;}// <TradingPlace>TA_1583123</TradingPlace>
    public DateTime ValDate {get;set;}// <ValDate>2020-11-19</ValDate>
    public double SecQty {get;set;}// <SecQty>76</SecQty>
    public double ExecPrice {get;set;}// <ExecPrice>156.76</ExecPrice>
    public string ExecPriceCcy {get;set;}// <ExecPriceCcy>EUR</ExecPriceCcy>
    public string TradeCcy {get;set;}// <TradeCcy>EUR</TradeCcy>
    public string Counterparty {get;set;}// <Counterparty>BROWN BROTHERS HARRIMAN FUND ADMINISTRATION SERVICES (IRELAND) LTD</Counterparty>
    public double? GrossAmount {get;set;}// <GrossAmount>-11913.76</GrossAmount>

    public double? NetAmount {get;set;}// <NetAmount>-11913.76</NetAmount>
    public string BookCcy {get;set;}// <BookCcy>EUR</BookCcy>
    public double? BankFee {get;set;} //<BankFee>-82.9</BankFee>
    public string Iban {get;set;}// <Iban>LU870080399837602001</Iban>
    public double? Xrate {get;set;}// <Xrate>1.183921</Xrate>
    public string DepositaryBic {get;set;}// <DepositaryBic>CHASLULX</DepositaryBic>
    public string DepositaryName {get;set;}// <DepositaryName>JPMORGAN BANK Luxembourg SA</DepositaryName>
    public string Communication1{get;set;} //<Communication1>Subscription (Funds) Equity funds</Communication1>
    
    public string FileName { get; set; }
}
class BdlCashtrNode
{
    public string ContId { get; set; } // /response/response/cont/@ContId
    public string PosId { get; internal set; }
    public string Iban{get;set;} // <Iban>LU880080370142002001</Iban>
    public string OrderNr{get;set;} // <OrderNr>283474336</OrderNr>
    public string OrdTypId{get;set;} // <OrdTypId>52011</OrdTypId>
    public string ExternalRef{get;set;} // <ExternalRef/>
    public string BookText{get;set;} // <BookText>FX Spot for PB: USD -670.94 (EUR/USD 1.188364)</BookText>
    public string ContractNr{get;set;} // <ContractNr/>
    public double? GrossAmt{get;set;} // <GrossAmt/>
    public string GrossAmtCcy{get;set;} // <GrossAmtCcy/>
    public double? Xrate{get;set;} // <Xrate>1.188364</Xrate>
    public double? fwdRate{get;set;} // <fwdRate>1.188364</fwdRate>
    public double? NdfShortAmt{get;set;} // <NdfShortAmt>670.94</NdfShortAmt>
    public string NdfShortAmtCcy{get;set;} // <NdfShortAmtCcy>USD</NdfShortAmtCcy>
    public double? NdfLongAmt{get;set;} // <NdfLongAmt>564.59</NdfLongAmt>
    public string NdfLongAmtCcy{get;set;} // <NdfLongAmtCcy>EUR</NdfLongAmtCcy>
    public double? NdfValuePrice{get;set;} // <NdfValuePrice>1.186200</NdfValuePrice>
    public DateTime? NdfConclDate{get;set;} // <NdfConclDate>2020-11-17</NdfConclDate>
    public string BookCcy{get;set;} // <BookCcy>EUR</BookCcy>
    public double? FeeComm{get;set;} // <FeeComm/>
    public double? NetAmount{get;set;} // <NetAmount>564.59</NetAmount>
    public DateTime bookDate{get;set;} // <bookDate>2020-11-17</bookDate>
    public DateTime ValDate{get;set;} // <ValDate>2020-11-19</ValDate>
    public string ReverseInd{get;set;} // <ReverseInd>Y</ReverseInd>

    public string Communication1{get;set;} // <Communication1>FRAIS D'ADMINISTRATION + PRIME DE</Communication1>
    public string Communication2{get;set;} // <Communication2>RISQUE  1ER TRIM 2020</Communication2>
    public string Communication3{get;set;} 
    public string Communication4{get;set;} 
    
    public string PayBenIban{get;set;}  //<PayBenIban>BE10363050436404</PayBenIban>
    public string PayBenAddr1{get;set;} //<PayBenAddr1>VITIS LIFE SA</PayBenAddr1>
    public string PayBenAddr2{get;set;} //<PayBenAddr2>52 BOULEVARD MARCEL CAHEN</PayBenAddr2>
    public string PayBenAddr3{get;set;} //<PayBenAddr3>L-1311 LUXEMBOURG</PayBenAddr3>
    public string PayBenAddr4{get;set;} //<PayBenAddr4/>
}
class BdlXratesNode //<response><payload><parameters><Xrates>
{
     public string Currency{get;set;}//<Currency>AED</Currency>
     public double Xrate{get;set;}//<Xrate>4.315217</Xrate>
     public int Rounding{get;set;}//<Rounding>2</Rounding>
     public string Display{get;set;}//<Display>1 EUR = 4.315217 AED</Display>
     public double RevsXrate{get;set;}//<RevsXrate>0.231738</RevsXrate>
     public DateTime XrateDate{get;set;}//<XrateDate>2020-10-28</XrateDate>
}
class BdlTableNode //<response><payload><parameters><Table>
{
    public string TableCode{get;set;} //<TableCode>APPLICFORM</TableCode>
    public string KeyVal {get;set;} //<KeyVal>7333</KeyVal>
    public string Descr {get;set;} //<Descr>No form</Descr>
}

class BdlFileDefinition : XmlFileDefinition
{
    public BdlFileDefinition()
    {
        this.AddNodeDefinition(XmlNodeDefinition.Create("secbase", "/response/response/cont/pos/Secbase", i => new BdlSecBaseNode
        {   
            SecurityCode  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/SecurityCode"),
            Isin  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/Isin"),
            Telekurs  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/Telekurs"),
            Reuters  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/Reuters"),
            Bloomberg  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/Bloomberg"),
            Wkn  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/Wkn"),
            SecName  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/SecName"),
            MifidPTyp  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/MifidPTyp"),
            CFIclass  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/CFIclass"),
            CaaClassif = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/CaaClassif"),
            InstrType  = i.ToXPathQuery<int?>("/response/response/cont/pos/Secbase/InstrType"),
            InstrTypeL1  = i.ToXPathQuery<int?>("/response/response/cont/pos/Secbase/InstrTypeL1"),
            InstrTypeL2  = i.ToXPathQuery<int?>("/response/response/cont/pos/Secbase/InstrTypeL2"),
            EusdStatA = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/EusdStatA"), 
            TradingPlace = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/TradingPlace"), 
            WthldTax = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/WthldTax"), 
            EconSector  = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/EconSector"),
            GeoSector  = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/GeoSector"),
            Domicile  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/Domicile"),
            InstrCcy  = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/InstrCcy"),

            PriceUnit = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/PriceUnit"),
            QuotMode = i.ToXPathQuery<int>("/response/response/cont/pos/Secbase/QuotMode"),
            PriceCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/PriceCcy"),
            Price = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/Price"),
            PriceDate = i.ToXPathQuery<DateTime?>("/response/response/cont/pos/Secbase/PriceDate"),

            Issuer = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Issuer"),
            IssueDomic = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/IssueDomic"),

            IntrRate = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/IntrRate"),
            YieldToMat = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/YieldToMat"),
            FaceAmt = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/FaceAmt"),
            MatRdmptDate = i.ToXPathQuery<DateTime?>("/response/response/cont/pos/Secbase/MatRdmptDate"),
            NextCoupDate = i.ToXPathQuery<DateTime?>("/response/response/cont/pos/Secbase/NextCoupDate"),

            MifidRisk = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/MifidRisk"),
            MifidComplx = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/MifidComplx"),
            Mifid2Complx = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/Mifid2Complx"),

            EurAmStyle  = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/EurAmStyle "),
            UnderlAsset = i.ToXPathQuery<string>("/response/response/cont/pos/Secbase/UnderlAsset"),
            ContrSize  = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/ContrSize "),
            StrikePrice = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/StrikePrice"),

            ValFreq = i.ToXPathQuery<int?>("/response/response/cont/pos/Secbase/ValFreq"), 
            FundValRdmpt = i.ToXPathQuery<int?>("/response/response/cont/pos/Secbase/FundValRdmpt"), 
            FundValSubs = i.ToXPathQuery<int?>("/response/response/cont/pos/Secbase/FundValSubs"), 
            MinSubsAmt = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/MinSubsAmt"), 
            TechCutoff = i.ToXPathQuery<string >("/response/response/cont/pos/Secbase/TechCutoff"), 
            FrontEndLoad = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/FrontEndLoad"), 
            BackEndLoad = i.ToXPathQuery<double?>("/response/response/cont/pos/Secbase/BackEndLoad"), 
        }));
        this.AddNodeDefinition(XmlNodeDefinition.Create("secpos", "/response/response/cont/pos/Secpos", i => new BdlSecPosNode
        {
            PosId = i.ToXPathQuery<string>("/response/response/cont/pos/@PosId"),
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            SecurityCode  = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/SecurityCode"),
            Isin = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/Isin"),
            AssetQty = i.ToXPathQuery<double>("/response/response/cont/pos/Secpos/AssetQty"),
            AssBalDate = i.ToXPathQuery<DateTime>("/response/response/cont/pos/Secpos/AssBalDate"),
            PriceCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/PriceCcy"),
            UnitPrice = i.ToXPathQuery<double?>("/response/response/cont/pos/Secpos/UnitPrice"),
            PriceDate = i.ToXPathQuery<DateTime>("/response/response/cont/pos/Secpos/PriceDate"),
            CostPrice = i.ToXPathQuery<double?>("/response/response/cont/pos/Secpos/CostPrice"),
            aveCostCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/aveCostCcy"),
            UnReEur  = i.ToXPathQuery<double?>("/response/response/cont/pos/Secpos/UnReEur"),
            TotSec = i.ToXPathQuery<double>("/response/response/cont/pos/Secpos/TotSec"),
            TotSecCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/TotSecCcy"),
            TotRefCcy = i.ToXPathQuery<double>("/response/response/cont/pos/Secpos/TotRefCcy"),
            TotInClientCcy = i.ToXPathQuery<double?>("/response/response/cont/pos/Secpos/TotInClientCcy"),
            AccrIntRef = i.ToXPathQuery<double?>("/response/response/cont/pos/Secpos/AccrIntRef"),
            ClientCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/ClientCcy"),
            RefCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Secpos/RefCcy"),
            FileName = i.ToSourceName()
        }));
        this.AddNodeDefinition(XmlNodeDefinition.Create("cashpos", "/response/response/cont/pos/Cashpos", i => new BdlCashposNode
        {
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            PosId = i.ToXPathQuery<string>("/response/response/cont/pos/@PosId"),
            Iban = i.ToXPathQuery<string>("/response/response/cont/pos/Cashpos/Iban"),
            AssetCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Cashpos/AssetCcy"),
            AssetType = i.ToXPathQuery<int>("/response/response/cont/pos/Cashpos/AssetType"),
            ContractNr = i.ToXPathQuery<string>("/response/response/cont/pos/Cashpos/ContractNr"),
            PosBalDate = i.ToXPathQuery<DateTime>("/response/response/cont/pos/Cashpos/PosBalDate"),
            PosBalRefCcy = i.ToXPathQuery<double>("/response/response/cont/pos/Cashpos/PosBalRefCcy"),
            balBook = i.ToXPathQuery<double>("/response/response/cont/pos/Cashpos/balBook"),
            balVal = i.ToXPathQuery<double>("/response/response/cont/pos/Cashpos/balVal"),
            AccrInt = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashpos/AccrInt"),
            FileName = i.ToSourceName()
        }));
        this.AddNodeDefinition(XmlNodeDefinition.Create("custid", "/response/response/cont/Custid", i => new BdlCustidNode
        {
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            CollectionId = i.ToXPathQuery<string>("/response/response/cont/Custid/CollectionId"),
            DefaultCcy = i.ToXPathQuery<string>("/response/response/cont/Custid/DefaultCcy"),
            Domicile = i.ToXPathQuery<string>("/response/response/cont/Custid/Domicile"),
            OpenDate = i.ToXPathQuery<DateTime>("/response/response/cont/Custid/OpenDate"),
            TaxDomicile = i.ToXPathQuery<string>("/response/response/cont/Custid/TaxDomicile"),
            EusdStatus = i.ToXPathQuery<int?>("/response/response/cont/Custid/EusdStatus"),
            InitMifid = i.ToXPathQuery<string>("/response/response/cont/Custid/InitMifid"),
            FinKnowl = i.ToXPathQuery<int?>("/response/response/cont/Custid/FinKnowl"),
            FinExp = i.ToXPathQuery<int?>("/response/response/cont/Custid/FinExp"),  
        }));

        this.AddNodeDefinition(XmlNodeDefinition.Create("Sectrans", "/response/response/cont/pos/Sectrans", i => new BdlSectransNode
        {
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            PosId = i.ToXPathQuery<string>("/response/response/cont/pos/@PosId"),
            ContIdSec =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/ContIdSec"),
            OrderNr =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/OrderNr"),
            SecurityCode =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/SecurityCode"),
            Isin =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/Isin"),
            OrdTypId =i.ToXPathQuery<string>("/response/response/cont/pos/Sectrans/OrdTypId"),
            OrderDate =i.ToXPathQuery<DateTime >("/response/response/cont/pos/Sectrans/OrderDate"),
            ExecDate =i.ToXPathQuery<DateTime >("/response/response/cont/pos/Sectrans/ExecDate"),
            TradingPlace =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/TradingPlace"),
            ValDate =i.ToXPathQuery<DateTime >("/response/response/cont/pos/Sectrans/ValDate"),
            SecQty =i.ToXPathQuery<double >("/response/response/cont/pos/Sectrans/SecQty"),
            ExecPrice =i.ToXPathQuery<double >("/response/response/cont/pos/Sectrans/ExecPrice"),
            ExecPriceCcy =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/ExecPriceCcy"),
            TradeCcy =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/TradeCcy"),
            Counterparty =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/Counterparty"),
            GrossAmount =i.ToXPathQuery<double?>("/response/response/cont/pos/Sectrans/GrossAmount"),
            NetAmount =i.ToXPathQuery<double?>("/response/response/cont/pos/Sectrans/NetAmount"),
            BankFee =i.ToXPathQuery<double?>("/response/response/cont/pos/Sectrans/BankFee"),
            BookCcy =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/BookCcy"),
            Iban =i.ToXPathQuery<string >("/response/response/cont/pos/Sectrans/Iban"),
            Xrate =i.ToXPathQuery<double?>("/response/response/cont/pos/Sectrans/Xrate"),
            DepositaryBic = i.ToXPathQuery<string>("/response/response/cont/pos/Sectrans/DepositaryBic"),
            DepositaryName = i.ToXPathQuery<string>("/response/response/cont/pos/Sectrans/DepositaryName"),
            Communication1 = i.ToXPathQuery<string>("/response/response/cont/pos/Sectrans/Communication1"),
        }));


        this.AddNodeDefinition(XmlNodeDefinition.Create("Cashtr", "/response/response/cont/pos/Cashtr", i => new BdlCashtrNode
        {
            ContId = i.ToXPathQuery<string>("/response/response/cont/@ContId"),
            PosId = i.ToXPathQuery<string>("/response/response/cont/pos/@PosId"),
            Iban = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/Iban"),
            OrderNr = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/OrderNr"),
            OrdTypId = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/OrdTypId"),
            ExternalRef = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/ExternalRef"),
            BookText = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/BookText"),
            ContractNr = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/ContractNr"),
            GrossAmt = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/GrossAmt"),
            GrossAmtCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/GrossAmtCcy"),
            Xrate = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/Xrate"),
            fwdRate = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/fwdRate"),
            NdfShortAmt = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/NdfShortAmt"),
            NdfShortAmtCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/NdfShortAmtCcy"),
            NdfLongAmt = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/NdfLongAmt"),
            NdfLongAmtCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/NdfLongAmtCcy"),
            NdfValuePrice = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/NdfValuePrice"),
            NdfConclDate = i.ToXPathQuery<DateTime?>("/response/response/cont/pos/Cashtr/NdfConclDate"),
            BookCcy = i.ToXPathQuery<string>("/response/response/cont/pos/Cashtr/BookCcy"),
            FeeComm = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/FeeComm"),
            NetAmount = i.ToXPathQuery<double?>("/response/response/cont/pos/Cashtr/NetAmount"),
            bookDate = i.ToXPathQuery<DateTime >("/response/response/cont/pos/Cashtr/bookDate"),
            ValDate = i.ToXPathQuery<DateTime >("/response/response/cont/pos/Cashtr/ValDate"),
            ReverseInd = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/ReverseInd"),

            Communication1 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/Communication1"),
            Communication2 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/Communication2"),
            Communication3 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/Communication3"),
            Communication4 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/Communication4"),

            PayBenIban = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/PayBenIban"),
            PayBenAddr1 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/PayBenAddr1"),
            PayBenAddr2 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/PayBenAddr2"),
            PayBenAddr3 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/PayBenAddr3"),
            PayBenAddr4 = i.ToXPathQuery<string >("/response/response/cont/pos/Cashtr/PayBenAddr4"),
        }));

        this.AddNodeDefinition(XmlNodeDefinition.Create("Xrates", "/response/response/payload/parameters/Xrates", i => new BdlXratesNode
        {
            Currency = i.ToXPathQuery<string>("/response/response/payload/parameters/Xrates/Currency"),
            Xrate = i.ToXPathQuery<double>("/response/response/payload/parameters/Xrates/Xrate"),
            Rounding = i.ToXPathQuery<int>("/response/response/payload/parameters/Xrates/Rounding"),
            Display = i.ToXPathQuery<string>("/response/response/payload/parameters/Xrates/Display"),
            RevsXrate = i.ToXPathQuery<double>("/response/response/payload/parameters/Xrates/RevsXrate"),
            XrateDate = i.ToXPathQuery<DateTime>("/response/response/payload/parameters/Xrates/XrateDate"),            
        }));

        this.AddNodeDefinition(XmlNodeDefinition.Create("Table", "/response/response/payload/parameters/Table", i => new BdlTableNode
        {
            TableCode = i.ToXPathQuery<string>("/response/response/payload/parameters/Table/TableCode"),
            KeyVal  = i.ToXPathQuery<string>("/response/response/payload/parameters/Table/KeyVal"),
            Descr  = i.ToXPathQuery<string>("/response/response/payload/parameters/Table/Descr"),
        }));
    }
}
#endregion

#region GET FILE STREAMS
var nodesStream = FileStream
    .CrossApplyXmlFile($"{TaskName}: parse input file", new BdlFileDefinition());

var filePortfoliosStream = nodesStream
    .XmlNodeOfType<BdlCustidNode>($"{TaskName}: list only portfolios")
    .SetForCorrelation($"{TaskName}: correlate portfolios")
    .Distinct($"{TaskName}: distinct portfolio", i => i.ContId);

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

var fileSecurityTransationsStream = nodesStream
    .XmlNodeOfType<BdlSectransNode>($"{TaskName}: list only security transactions")
    .SetForCorrelation($"{TaskName}: correlate security transactions");

var fileCashMovStream = nodesStream
    .XmlNodeOfType<BdlCashtrNode>($"{TaskName}: list only cash movements")
    .SetForCorrelation($"{TaskName}: correlate cash movements");

var fileXratesStream = nodesStream
    .XmlNodeOfType<BdlXratesNode>($"{TaskName}: list only xrates nodes")
    .SetForCorrelation($"{TaskName}: correlate Xrates nodes");

var fileTableStream = nodesStream
    .XmlNodeOfType<BdlTableNode>($"{TaskName}: list only table nodes");
    //.SetForCorrelation($"{TaskName}: correlate table nodes");
#endregion

#region PORTFOLIOS / PERSONS-COMPANIES / INVESTOR RELATIONSHIPS
var portfolioStream = filePortfoliosStream
    .LookupCurrency($"{TaskName}: get related currency for portfolio", l => l.DefaultCcy, (l, r) => new { l.ContId, CurrencyId = r?.Id, fileRow=l })
    .Select($"{TaskName}: create portfolio", i => new DiscretionaryPortfolio
    {
        InternalCode = GetPortfolioInternalCode(i.ContId),
        Name = $"{i.ContId}",
        ShortName = $"{i.fileRow.CollectionId}",
        CurrencyId = i.CurrencyId,
        InceptionDate = i.fileRow.OpenDate,
        PricingFrequency = FrequencyType.Daily,
    })
    .EfCoreSave($"{TaskName}: save portfolio", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var personStream = filePortfoliosStream
    .Where($"{TaskName}: filter individual client", i=>i.InitMifid.Contains("(01)"))
    .LookupCountry($"{TaskName}: get related country for person", l => l.Domicile, (l, r) => new {fileRow = l, CountryId = r?.Id})
    .LookupCurrency($"{TaskName}: get related currency for person", l => l.fileRow.DefaultCcy, (l, r) => 
                new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = r?.Id})
    .Select($"{TaskName}: create person entity", i => new Person
    {
        InternalCode = GetPortfolioInternalCode(i.fileRow.ContId),
        FirstName = "",
        LastName = $"{i.fileRow.ContId}",
        CurrencyId = i.CurrencyId.HasValue? i.CurrencyId.Value:(int?)null,
        CountryId = i.CountryId.HasValue? i.CountryId.Value:(int?)null,
        Culture = new CultureInfo("en-GB"),
    })
    .EfCoreSave($"{TaskName}: save person", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var individualInvestorRelationshipStream = personStream
    .Select($"{TaskName} create individual Investor Relationship", i => 
        new {EntityInternalCode=i.InternalCode, Relationship= new InvestorRelationship{
        EntityId = i.Id,
        InvestorType = InvestorType.Retail,
        StatementFrequency = FrequencyType.Quarterly,
        StartDate = DateTime.Today,
        CurrencyId = i.CurrencyId.Value,
    }})
    .EfCoreSave($"{TaskName}: Save Individual Investor Relationship", o => o
        .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));
        
var companyStream = filePortfoliosStream
    .Where($"{TaskName}: filter company client", i=> !i.InitMifid.Contains("(01)"))
    .LookupCountry($"{TaskName}: get related country for companies", l => l.Domicile, (l, r) => new {fileRow = l, CountryId = r?.Id})
    .LookupCurrency($"{TaskName}: get related currency for companies", l => l.fileRow.DefaultCcy, (l, r) => 
                new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = r?.Id})
    .Select($"{TaskName}: create client company entity", i => new Company
    {
        InternalCode = GetPortfolioInternalCode(i.fileRow.ContId),
        Name = $"{i.fileRow.ContId}",
        CurrencyId = i.CurrencyId.HasValue? i.CurrencyId.Value:(int?)null,
        CountryId = i.CountryId.HasValue? i.CountryId.Value:(int?)null,
        Culture = new CultureInfo("en-GB"),
        YearEnd = new DateOfYear(12,31)
    }).EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var companyInvestorRelationshipStream = companyStream
    .CorrelateToSingle($"{TaskName}: get related company  file row", filePortfoliosStream, (l, r) => new { Company = l, FileRow=r})
    .Select($"{TaskName} create company Investor Relationship", i =>
     new {EntityInternalCode=i.Company.InternalCode, Relationship= new InvestorRelationship{
        EntityId = i.Company.Id,
        InvestorType = i.FileRow.InitMifid.Contains("(03)")? InvestorType.Institutional:InvestorType.Retail,
        StatementFrequency = FrequencyType.Quarterly,
        StartDate = i.FileRow.OpenDate,
        CurrencyId = i.Company.CurrencyId.Value,
    }})
    .EfCoreSave($"{TaskName}: Save Company Investor Relationship", o => o
        .Entity(i=>i.Relationship).SeekOn(i => i.EntityId).DoNotUpdateIfExists().Output((i,e)=> i));

var investorsStream = individualInvestorRelationshipStream
    .Union($"{TaskName}: merge of the investor relationship streams", companyInvestorRelationshipStream);

var relationshipPortfoliosStream = investorsStream
	.Lookup($"{TaskName}: Link Investor-Porfolio - get related portfolio",portfolioStream, 
                i=>i.EntityInternalCode,i=>i.InternalCode, (l,r) => new {InvestorRelationship = l.Relationship, Portfolio = r})
	.Where($"{TaskName}: Link Investor-Porfolio - Filter existing portfolio", i=>i.Portfolio !=null)
    .Select($"{TaskName}: create link between investor and related portfolio", i => new RelationshipPortfolio {
		RelationshipId = i.InvestorRelationship.Id, PortfolioId=i.Portfolio.Id})
	.EfCoreSave($"{TaskName}: Save link Relationship-Portfolio", 
        o => o.SeekOn(i => new {i.RelationshipId, PortfolioId=i.PortfolioId}).DoNotUpdateIfExists());
#endregion

#region INVESTOR CLASSIFICATIONS
// 1. MIFIDCLASS Type definition
var MIFIDCLASSType = ProcessContextStream
    .Select($"{TaskName}: Create MIFIDCLASS classification type", ctx => new InvestorClassificationType 
    { 
        Code = "MIFIDCLASS-BDL", 
        Name = new MultiCultureString { ["en"] = "Investor MiFID Classification - BDL" }
    })
    .EfCoreSave($"{TaskName}: Save MIFIDCLASS type", o => o.SeekOn(ct => ct.Code)
    .DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure MIFIDCLASS is single");

// 2. MIFIDCLASS: Investor Classification definition
var MIFIDCLASSClassificationStream = filePortfoliosStream
    .Distinct($"{TaskName}: Distinct MIFIDCLASS", i => i.InitMifid)
    .Select($"{TaskName}: Create MIFIDCLASS classification", MIFIDCLASSType, (i, t) => new InvestorClassification
    {
        Code = i.InitMifid,
        Name = new MultiCultureString { ["en"] = i.InitMifid },
        ClassificationTypeId = t.Id
    })
    .EfCoreSave($"{TaskName}: Save MIFIDCLASS Classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code })
    .DoNotUpdateIfExists());

// 3. MIFIDCLASS: Investor classification assignations
var MIFIDCLASSAssignations = filePortfoliosStream
	.Lookup($"{TaskName}: Get related investor", investorsStream, 
                i=> GetPortfolioInternalCode(i.ContId),i=>i.EntityInternalCode,(l, r) => new{FileRow = l,InvestorRelationship=r})
	.Lookup($"{TaskName}: Get related investor classification", MIFIDCLASSClassificationStream
            , i=>i.FileRow.InitMifid, i=>i.Code, (l,r) => 
            new { FileRow = l.FileRow, InvestorRelationship = l.InvestorRelationship, InvestorClassification = r})
    .Select($"{TaskName}: Investor classification",i=> new ClassificationOfInvestorRelationship
        { 
            InvestorRelationshipId= (i.InvestorRelationship !=null)? i.InvestorRelationship.Relationship.Id
                            : throw new Exception("InvestorRelationship is null: " + i.FileRow.ContId ),
            ClassificationTypeId = i.InvestorClassification.ClassificationTypeId,
            ClassificationId = i.InvestorClassification.Id
        })
    .EfCoreSave($"{TaskName}: Save Investor classification assignation", 
                o => o.SeekOn(i => new { i.ClassificationTypeId, i.InvestorRelationshipId})
    .DoNotUpdateIfExists());
#endregion

#region TARGET SECURITIES

var issuerSicavsStream = fileTargetSecuritiesStream
    .Where($"Filter non Share Classes for SICAV", i=>IsShareClassInstrType(i.InstrType.Value))
    .Distinct($"{TaskName}: distinct SecBase SICAV", i => GetSicavName(i.Issuer,i.SecName))
    .LookupCountry($"{TaskName}: get Sicav related country", l => l.IssueDomic, 
        (l,r) => new {FileRow = l, IssuerCountry=r })
    .Select($"{TaskName}: Create Issuer SICAV", i => new Sicav{
        InternalCode = GetSicavName(i.FileRow.Issuer,i.FileRow.SecName),
        Name = GetSicavName(i.FileRow.Issuer,i.FileRow.SecName),
        CountryId = (i.IssuerCountry != null)? i.IssuerCountry.Id : (int?) null,
    })
    .EfCoreSave($"{TaskName}: save target issuer Sicavs", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var issuerSicavsStreamFixIssuer = issuerSicavsStream
    .Fix($"{TaskName}: IssuerId ", i => i.FixProperty(i => i.IssuerId).AlwaysWith(i => i.Id))
    .EfCoreSave("Fixing Sicav issuer Id");

var issuerCompaniesStream = fileTargetSecuritiesStream
    .Where($"Filter Share Classes", i=> !IsShareClassInstrType(i.InstrType.Value))
    .Distinct($"{TaskName}: distinct SecBase Issuers Companies", i => GetIssuerInternalCode(i.Issuer,i.SecName,i.InstrType.Value))
    .LookupCountry($"{TaskName}: get Sicav related companies country", l => l.IssueDomic, (l,r) => new {FileRow = l, IssuerCountry=r })
    .Select($"{TaskName}: Create Issuer companies",i=> new Company{
        InternalCode = GetIssuerInternalCode(i.FileRow.Issuer,i.FileRow.SecName,i.FileRow.InstrType.Value),
        Name =  GetIssuerInternalCode(i.FileRow.Issuer,i.FileRow.SecName,i.FileRow.InstrType.Value),
        CountryId = (i.IssuerCountry != null)? i.IssuerCountry.Id : (int?) null,
    })
    .EfCoreSave($"{TaskName}: save target issuer companies", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSubFundsStream = fileTargetSecuritiesStream
    .Where($"Filter Share Class for Sub fund",i=>IsShareClassInstrType(i.InstrType.Value) && !string.IsNullOrEmpty(i.Issuer))
    .Distinct($"{TaskName}: distinct SecBase Sub-Funds", i => GetSubFundName(i.Issuer,i.SecName))
    .Lookup($"{TaskName}: get related sub-fund Sicav", issuerSicavsStream, 
        i => GetSicavName(i.Issuer,i.SecName), i => i.InternalCode,
        (l,r) => new {FileRow = l, Sicav = r })
    .LookupCountry($"{TaskName}: get related sub fund country", i => i.FileRow.Domicile, 
        (l,r) => new {FileRow = l.FileRow, Sicav= l.Sicav, Country=r })
    .LookupCurrency($"{TaskName}: get related sub fund currency", i => i.FileRow.InstrCcy , 
        (l,r) => new {FileRow = l.FileRow, Sicav= l.Sicav, Country= l.Country, Currency = r })
    .Select($"{TaskName}: Create target subFund ", i => new SubFund{
        InternalCode =  GetSubFundName(i.FileRow.Issuer,i.FileRow.SecName),
        Name =  GetSubFundName(i.FileRow.Issuer,i.FileRow.SecName),
        ShortName  =  "from BDL",
        CountryId = (i.Country != null)? i.Country.Id : (int?) null,
        DomicileId = (i.Country != null)? i.Country.Id : (int?) null,
        SicavId = i.Sicav.Id,
        SettlementNbDays = i.FileRow.FundValRdmpt,
        CutOffTime = TimeSpan.TryParse(i.FileRow.TechCutoff,out var res)?res: (TimeSpan?) null, //<TechCutoff>14:15</TechCutoff>
        PricingFrequency = GetPricingFrequency(i.FileRow.ValFreq),
    })
    .EfCoreSave($"{TaskName}: save target sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var targetSecurityInstrumentStream = fileTargetSecuritiesStream
    .Distinct($"{TaskName}: distinct target positions security", i => i.Isin ?? i.SecurityCode)
    .LookupCurrency($"{TaskName}: get related currency for target security", l => l.InstrCcy, 
        (l, r) => new {FileRow =l,Currency = r })
    .LookupCountry($"{TaskName}: get related country for target security", l => l.FileRow.Domicile, 
        (l, r) => new {FileRow = l.FileRow ,Currency = l.Currency,Country = r  })
    .Lookup($"{TaskName}: lookup share class sub-fund",targetSubFundsStream,
        i=> GetSubFundName(i.FileRow.Issuer,i.FileRow.SecName), i=>i.InternalCode,
        (l, r) => new {FileRow = l.FileRow ,Currency = l.Currency,Country = l.Country, SubFund = r })
    .Select($"{TaskName}: create target security", i => CreateTargetSecurity(i.FileRow, i.Currency, i.Country, i.SubFund))
    //.WhereCorrelated($"{TaskName}: keep known security instrument types", i => i != null)
    .EfCoreSave($"{TaskName}: save target security", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

#endregion

#region TARGET SECURITY CLASSIFICATIONS
var classificationTypesStream = ProcessContextStream.CrossApplyEnumerable($"{TaskName}: Create BDL Classifications Types",ctx=>
    new [] {
        new { Code = "InstrType-BDL", Name = "Instrument Type - BDL", TableCode="SECTYP" },
        new { Code = "InstrTypeL1-BDL", Name = "Instrument Type Level 1 - BDL", TableCode="SECTYP"},
        new { Code = "InstrTypeL2-BDL", Name = "Instrument Type Level 2 - BDL", TableCode="SECTYP"},
        new { Code = "MifidPTyp-BDL", Name = "MifidPTyp - BDL", TableCode=""},
        new { Code = "MifidRisk-BDL", Name = "MifidRisk - BDL", TableCode=""},
        new { Code = "Mifid2Complx-BDL", Name = "Mifid2Complx - BDL", TableCode=""},
        new { Code = "CaaClassif", Name = "Caa Classification", TableCode="CAACLASS"},
        new { Code = "EconSector-BDL", Name = "Economic Sector - BDL", TableCode="ECOSEC"},
        new { Code = "GeoSector-BDL", Name = "Geographic Sector - BDL", TableCode="GEOSEC"},
        new { Code = "WthldTax", Name = "withholding tax class", TableCode="WITHHTAX"},
        new { Code = "TradingPlace", Name = "Market Place - BDL", TableCode="MKTPLACE"},
        new { Code = "EusdStatA", Name = "EUSD relevant - BDL", TableCode="EUSD"},
    })
    .Select($"{TaskName}: Create BDL Classifications Type Instances",i => new{
                TableCode=i.TableCode, 
                ClassificationType= new SecurityClassificationType { Code = i.Code, Name = new MultiCultureString { ["en"] = i.Name }}})
    .EfCoreSave($"{TaskName}: Save Classification Types", o => o
        .Entity(i=>i.ClassificationType)
        .SeekOn(ct => ct.Code).DoNotUpdateIfExists().Output((i,e)=> i));

var classificationStream = fileTargetSecuritiesStream
    .CrossApplyEnumerable($"{TaskName}: Cross apply BDL Classifications", i => 
        new [] {
            new{ TypeCode = "InstrType-BDL", ClassificationCode= i.InstrType.ToString()},
            new{ TypeCode = "InstrTypeL1-BDL", ClassificationCode= i.InstrTypeL1.ToString()},
            new{ TypeCode = "InstrTypeL2-BDL", ClassificationCode= i.InstrTypeL2.ToString()},
            new{ TypeCode = "MifidPTyp-BDL", ClassificationCode= i.MifidPTyp},
            new{ TypeCode = "MifidRisk-BDL", ClassificationCode= i.MifidRisk},
            new{ TypeCode = "Mifid2Complx-BDL", ClassificationCode= i.Mifid2Complx},
            new{ TypeCode = "CaaClassif", ClassificationCode= i.CaaClassif},
            new{ TypeCode = "EconSector-BDL", ClassificationCode= i.EconSector},
            new{ TypeCode = "GeoSector-BDL", ClassificationCode= i.GeoSector},
            new{ TypeCode = "WthldTax", ClassificationCode= i.WthldTax},
            new{ TypeCode = "TradingPlace", ClassificationCode= i.TradingPlace},
            new{ TypeCode = "EusdStatA", ClassificationCode  = i.EusdStatA},
    })
    .Where($"{TaskName}: Filter Classification null values", i => !string.IsNullOrEmpty(i.ClassificationCode))
    .Distinct($"{TaskName}: Distinct BDL Classifications", i => new { i.TypeCode, i.ClassificationCode})
    .Lookup($"{TaskName}: Get related classifications Type",classificationTypesStream,i=>i.TypeCode,i=>i.ClassificationType.Code,
            (l,r)=> new { ClassificationCode=l.ClassificationCode, Type= r} )
    .Lookup($"{TaskName}: Lookup related description in BDL dictionary", fileTableStream, 
            i=> new{ TableCode= i.Type.TableCode, KeyVal = i.ClassificationCode},i=>new{ TableCode=i.TableCode, KeyVal = i.KeyVal }, (l,r)=> 
            new {
                ClassificationCode=l.ClassificationCode, 
                Type= l.Type.ClassificationType,
                KeyVal = r
    })
    .Select($"{TaskName}: Create BDL Classifications", i=> new SecurityClassification
    {
        ClassificationTypeId = i.Type.Id,
        Code = i.ClassificationCode,
        Name = new MultiCultureString { ["en"] = (i.KeyVal !=null)? i.KeyVal.Descr : i.ClassificationCode }
    })
    .EfCoreSave($"{TaskName}: Save BDL classifications", o => o.SeekOn(i => new { i.Code })
    .DoNotUpdateIfExists());

var classificationAssignations = fileTargetSecuritiesStream
    .Lookup($"{TaskName}: Classification assign - Get related security", targetSecurityInstrumentStream, 
                i=>(!string.IsNullOrEmpty(i.Isin))?i.Isin:i.SecurityCode,i => i.InternalCode,
                (l, r) => new{FileRow = l,Security=r})
    .Where($"{TaskName}: Filter Non Cash",i=> i.Security!=null && !(i.Security is Cash))
    .CrossApplyEnumerable($"{TaskName}: Cross apply Security Classifications 2", i => 
        new [] {
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.InstrType.ToString()},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.InstrTypeL1.ToString()},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.InstrTypeL2.ToString()},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.MifidPTyp},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.MifidRisk},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.Mifid2Complx},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.CaaClassif},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.EconSector},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.GeoSector},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.WthldTax},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.TradingPlace},
            new{ Security = i.Security, FileRow = i.FileRow, Classification = i.FileRow.EusdStatA},
    })
    .Where($"{TaskName}: Assignation - Filter Classification null values", i => !string.IsNullOrEmpty(i.Classification))
    .Lookup($"{TaskName}: Get related classification", classificationStream, i=>i.Classification, i=>i.Code,
            (l,r)=> new { Security = l.Security, FileRow = l.FileRow, Classification = r})
    .Select($"{TaskName}: Assign classification",i=> new ClassificationOfSecurity 
        { 
            SecurityId = (i.Security!=null)? i.Security.Id
                            : throw new Exception("Security is null: " + i.FileRow.SecurityCode), 
            ClassificationTypeId = (i.Classification!=null)? i.Classification.ClassificationTypeId 
                            : throw new Exception("Classification not found for security: " + i.Security.InternalCode), 
            ClassificationId = i.Classification.Id 
        })
    .EfCoreSave($"{TaskName}: Save classification assignation", o => o.SeekOn(i => new { i.SecurityId, i.ClassificationTypeId })
    .DoNotUpdateIfExists());

#endregion

#region PORTFOLIO COMPOSITIONS
var allPositions = fileSecurityPositionsStream.Union($"{TaskName}: merge all cash and security positions", fileCashPositionsStream,
        (l, r) => new { Date = l.AssBalDate, ContId = l.ContId, SecurityPosition = l, CashPosition = r },
        (l, r) => new { Date = r.PosBalDate, ContId = r.ContId, SecurityPosition = l, CashPosition = r })
    .Lookup($"{TaskName}: get related portfolio", portfolioStream,
        i => GetPortfolioInternalCode(i.ContId),
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
#endregion

#region CASH SECURITIES
var targetCashStream = fileCashPositionsStream
    .Distinct($"{TaskName}: distinct target cash", i => i.Iban)
    .LookupCurrency($"{TaskName}: get related currency for target cash", l => l.AssetCcy, (l, r) => 
        new { l.Iban, l.AssetType, CurrencyId = r?.Id })
    .Select($"{TaskName}: create target cash", i => new Cash
    {
        InternalCode = i.Iban,
        Iban = i.Iban,
        CurrencyId = i.CurrencyId,
        Name = $"{i.Iban}",
        AccountType = GetCashAccountType(i.AssetType),
        ShortName = $"BDL_{i.Iban}".Truncate(MaxLengths.ShortName),
    })
    .EfCoreSave($"{TaskName}: save target cash", o => o.SeekOn(i => i.Iban).DoNotUpdateIfExists());
#endregion

#region CASH POSITIONS
var cashPositionToSaveStream = fileCashPositionsStream
    .Lookup($"{TaskName}: lookup target cash", targetCashStream, i => i.Iban, i => i.Iban, (l, r) => new { FromFile = l, Cash = r })
    .CorrelateToSingle($"{TaskName}: lookup cash portfolio composition", portfolioCompositionStream, (l, r) => new { l.FromFile, l.Cash, r.PortfolioComposition })
    .Select($"{TaskName}: create cash pos", i => new Position
    {
        PortfolioCompositionId = i.PortfolioComposition.Id,
        SecurityId = i.Cash.Id,
        Value = 1, //It's quantity (1 for cash position)
        MarketValueInSecurityCcy = i.FromFile.balVal,
        MarketValueInPortfolioCcy = i.FromFile.PosBalRefCcy,
        AccruedInterestInPortfolioCcy = i.FromFile.AccrInt.HasValue? i.FromFile.AccrInt.Value:(double?) null,
        //AccruedInterestInSecurityCcy=
        BookCostInSecurityCcy = i.FromFile.balBook,
        //BookCostInPortfolioCcy = 
    });
#endregion

#region SECURITY POSITIONS
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
        MarketValueInPortfolioCcy = i.FromFile.TotRefCcy,
        ValuationPrice = i.FromFile.UnitPrice,
        //AccruedInterestInSecurityCcy =
        AccruedInterestInPortfolioCcy = i.FromFile.AccrIntRef,
        CostPrice = i.FromFile.CostPrice,
        //BookCostInSecurityCcy =
        //BookCostInPortfolioCcy = 
        ProfitLossOnMarketPortfolioCcy = i.FromFile.UnReEur,
        // ProfitLossOnFxPortfolioCcy
    });

var savedPositions = cashPositionToSaveStream
    .Union($"{TaskName}: join to instrument positions", instrumentPositionsToSaveStream)
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));
#endregion

#region SECURITY TRANSACTIONS
var savedTransactionsStream = fileSecurityTransationsStream
    .Lookup($"{TaskName}: lookup transaction portfolio stream ", portfolioStream, i => i.ContId+"-BDL", i => i.InternalCode, (l, r) => new { FileRow = l, Portfolio = r })
    .Lookup($"{TaskName}: lookup related target security by Isin in transaction", targetSecurityInstrumentStream, 
            i => GetSecurityInternalCode(i.FileRow.Isin, i.FileRow.SecurityCode) , 
            i => i.InternalCode, (l, r) => new { FileRow = l.FileRow, Portfolio = l.Portfolio, Security1 = r })
    .Lookup($"{TaskName}: lookup related target security by Security Code in transaction", targetSecurityInstrumentStream, 
           i => i.FileRow.SecurityCode, i => i.InternalCode, (l, r) => 
           new { FileRow = l.FileRow, Portfolio = l.Portfolio, Security1 = l.Security1, Security2 = r })
    .Select($"{TaskName}: Create new security transaction", i => CreateSecurityTransaction(i.FileRow,i.Portfolio, i.Security1, i.Security2) )
    .EfCoreSave($"{TaskName}: Save security transaction", o => o.SeekOn(i => i.TransactionCode).DoNotUpdateIfExists());
#endregion

#region CASH MOVEMENTS
//CashMov Counterparty Companies
var fileCashMovCounterpartiesStream = fileCashMovStream
	.Where($"{TaskName}: filter cash mov with counterparty",i=> !string.IsNullOrEmpty(i.PayBenIban))
	.Distinct($"{TaskName}: Distinct cash mov counterparty company", i=> i.PayBenIban);

var cashMovCounterpartyCompaniesStream = fileCashMovCounterpartiesStream
	.Select($"{TaskName}: Create cash mov counterparty company", i => new Company
	{
		InternalCode = i.PayBenIban,
		Name = i.PayBenAddr1,
		StreetAddress = i.PayBenAddr2,
		Location = i.PayBenAddr3,
		Iban = i.PayBenIban,
    })
    .EfCoreSave($"{TaskName}: save target counterparty company", 
	    o => o.SeekOn(i => i.Iban).AlternativelySeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: Get euroCurrency", (i, j) => i.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: Ensures only one euro currency");

//CashMov Counterparty relationship
var cashMovCounterpartyRelationship  = fileCashMovCounterpartiesStream
    .LookupCurrency($"{TaskName}: get related counterparty ccy", i=>i.GrossAmtCcy, (l,r) => new {FileRow = l, Currency = r} )
    .Lookup($"{TaskName}: Get related counterparty company",cashMovCounterpartyCompaniesStream,
            i => i.FileRow.PayBenIban, i => !string.IsNullOrEmpty(i.Iban)? i.Iban : i.InternalCode,
            (l,r) => new {FileRow = l.FileRow, Currency= l.Currency, Company = r} )
	.Select($"{TaskName}: Create cash mov counterparty relationship",euroCurrency, (i,j) => new CounterpartyRelationship
	{
		EntityId = i.Company.Id,
        StartDate = DateTime.Today,
        LastAuthorizationChange = DateTime.Today,
        CurrencyId = (i.Currency!=null)? i.Currency.Id: j.Id,
    })
    .EfCoreSave($"{TaskName}: save target counterparty relationship", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());

var savedMovementStream = fileCashMovStream
    .LookupCurrency($"{TaskName}: Get cash movement related currency", l => l.BookCcy, (l, r) => new { FileRow = l, Currency = r })
    .Lookup($"{TaskName}: lookup mov portfolio stream ", portfolioStream, 
        i => GetPortfolioInternalCode(i.FileRow.ContId), i => i.InternalCode, 
        (l, r) =>  new { FileRow = l.FileRow,Currency = l.Currency, Portfolio = r })
    .Lookup($"{TaskName}: lookup mov related cash ", targetCashStream, i => i.FileRow.Iban, i => i.Iban, (l, r) => new { FileRow=l.FileRow,Currency = l.Currency, Portfolio = l.Portfolio, Cash = r})    
    //.EfCoreLookup($"{TaskName}: get cash security by IBAN", o => o.Set<Cash>().On(i => i.FileRow.Iban, i => i.Iban).Select((l, r) => new { FileRow=l.FileRow,Currency = l.Currency, Portfolio = l.Portfolio, Cash = r }).CacheFullDataset())    
    .Lookup($"{TaskName}: lookup related security transaction by OrderNr", savedTransactionsStream, 
            i => i.FileRow.ContId +"-" + i.FileRow.OrderNr, i => i.TransactionCode
            , (l, r) => new { FileRow=l.FileRow,Currency = l.Currency, Portfolio = l.Portfolio, Cash = l.Cash, SecTrans= r })
    .Select($"{TaskName}: Create new movement", i => CreateCashMovement(i.FileRow, i.Currency, i.Portfolio, i.Cash,i.SecTrans))
    .EfCoreSave($"{TaskName}: Save cash movement", o => o.SeekOn(i => new{i.TransactionCode,i.MovementCode}).DoNotUpdateIfExists());
#endregion

#region CASH MOVEMENT CLASSIFICATIONS
// 1. ORDTYP Type definition
var orderTypType = ProcessContextStream
    .Select($"{TaskName}: Create ORDERTYP movement classification type", ctx => new MovementClassificationType 
    { 
        Code = "orderTyp", 
        Name = new MultiCultureString { ["en"] = "Order type" }
    })
    .EfCoreSave($"{TaskName}: Save ORDERTYP type", o => o.SeekOn(ct => ct.Code)
    .DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure ORDERTYP is single");

// 2. ORDTYP: Movement Classification definition
var ordTypFileTableStream = fileTableStream.Where($"{TaskName} Filter FileTableStream for ORDTYP",i=>i.TableCode=="ORDTYP");
var orderTypClassificationStream = fileCashMovStream
    .Distinct($"{TaskName}: Distinct ORDTYP", i => i.OrdTypId)
	.Lookup($"{TaskName}: Lookup ORDTYP related description in BDL dictionary", ordTypFileTableStream, 
            i=> new{ KeyVal = i.OrdTypId},i=>new{KeyVal = i.KeyVal }, (l,r)=> new { FileRow=l, TableNode = r})
    .Select($"{TaskName}: Create ORDTYP movement classification", orderTypType, (i, t) => new MovementClassification
    {
        Code = i.FileRow.OrdTypId,
        Name = new MultiCultureString { ["en"] = (i.TableNode !=null)? i.TableNode.Descr : i.FileRow.OrdTypId},
        ClassificationTypeId = t.Id
    })
    .EfCoreSave($"{TaskName}: Save ORDTYP Classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code })
    .DoNotUpdateIfExists());

// 3. ORDTYP: Movement classification assignations
var orderTypAssignations = fileCashMovStream
	.Lookup($"{TaskName}: Mov classification assign - Get related movement", savedMovementStream, 
                i=> GetMovementCode(i.ContId, i.OrderNr, i.OrdTypId, i.NetAmount, i.bookDate),
                i=>i.MovementCode,(l, r) => new{FileRow = l,Movement=r})
	.Lookup($"{TaskName}: Mov classification assign - Get related classification", orderTypClassificationStream
            , i=>i.FileRow.OrdTypId, i=>i.Code, (l,r)=> new { FileRow = l.FileRow, Movement = l.Movement, MovClassification = r})
        .Select($"{TaskName}: Movement Assign classification",i=> new ClassificationOfCashMovement
        {  
            CashMovementId= (i.Movement!=null)? i.Movement.Id
                    : throw new Exception("Movement is null: "+GetMovementCode(i.FileRow.ContId, i.FileRow.OrderNr, i.FileRow.OrdTypId, i.FileRow.NetAmount, i.FileRow.bookDate)), 
            ClassificationTypeId = i.MovClassification.ClassificationTypeId,
            ClassificationId = i.MovClassification.Id 
        })
    .EfCoreSave($"{TaskName}: Save Movement classification assignation", o => o.SeekOn(i => new { i.ClassificationTypeId, i.CashMovementId })
    .DoNotUpdateIfExists());

#endregion

#region MARKET DATA
//Market prices
var securityPricesStream = fileTargetSecuritiesStream
	.Where($"{TaskName}: Filter null prices", i => i.Price.HasValue && !IsCashInstrType(i.InstrType.Value))
	.Distinct($"{TaskName}: distinct security prices", i=> new {i.SecurityCode, i.PriceDate, i.Price})
	.Lookup($"{TaskName}: Lookup price security",targetSecurityInstrumentStream,
		i => GetSecurityInternalCode(i.Isin,i.SecurityCode), i => i.InternalCode, (l,r) => new {FileRow = l, Security = r})
	.Select($"{TaskName}: Create security price", i => new SecurityHistoricalValue{
		SecurityId= i.Security.Id,
		Date = i.FileRow.PriceDate.Value,
		Type = HistoricalValueType.MKT,
		Value = i.FileRow.Price.Value,
	})
	.EfCoreSave($"{TaskName}: Save security price", o => o.SeekOn(i => new {i.SecurityId, i.Type, i.Date}).DoNotUpdateIfExists());

//FX RATES
var fxRatesStream = fileXratesStream
	.LookupCurrency($"{TaskName}: get Fx Rate currency to", i=> i.Currency, (l,r) => new {FileRow = l, CurrencyTo=r})
    .Where("${TaskName} filter unexisting currency", i=>i.FileRow.XrateDate!=null && i.CurrencyTo!=null)
	.Select($"{TaskName}: Create Fx Rate", i => new FxRate{
		CurrencyToId = i.CurrencyTo.Id,
		Date = i.FileRow.XrateDate,
		RateFromReferenceCurrency = i.FileRow.Xrate, //!<Display>1 EUR = 552.486188 ZRN</Display>
	})
	.EfCoreSave($"{TaskName}: Save fx rate", o => o.SeekOn(i => new {i.CurrencyToId, i.Date}).DoNotUpdateIfExists());
#endregion

// return FileStream.WaitWhenDone($"{TaskName}: wait end of all save", savedPositions);

#region Helpers
FrequencyType GetPricingFrequency(int? ValFreq)
    => (ValFreq !=null && ValFreq.Value == 7312)? FrequencyType.Weekly : FrequencyType.Daily; //<ValFreq>7310</ValFreq> 7310=daily, 7312=weekly

bool IsShareClassInstrType(int instrType)
    => (instrType >= 800 && instrType <= 890) || (instrType == 895);

bool IsCashInstrType(int instrType)
    => (instrType >= 900 && instrType < 1000);
Security CreateTargetSecurity(BdlSecBaseNode fileRow, Currency currency, Country country, SubFund subfund)
{
    Security security = null;
    if (IsShareClassInstrType(fileRow.InstrType.Value))
        security = new ShareClass();
    else if (IsCashInstrType(fileRow.InstrType.Value))
       security = new Cash(){Rate = 0, AccountType = AccountType.MarginAccount};
    else
        switch (fileRow.InstrType.Value)
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
            case 890:
                security = new Etf();
                break;
            default:
                throw new Exception($"Unknown instrument type {fileRow.InstrType.Value} not managed: (isin: {fileRow.Isin}, name: {fileRow.SecName})");
        }

    if (security == null) return null;
    security.CurrencyId = currency.Id;
    security.InternalCode = fileRow.SecurityCode; 
    security.Name = fileRow.SecName;
    security.ShortName = fileRow.SecName.Truncate(MaxLengths.ShortName);
    if (security is OptionFuture der)
        der.UnderlyingIsin = fileRow.Isin;

    if (security is SecurityInstrument securityInstrument)
    {
        securityInstrument.InternalCode = !string.IsNullOrEmpty(fileRow.Isin)? fileRow.Isin : fileRow.SecurityCode;
        securityInstrument.Isin = fileRow.Isin;
    }
    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.CountryId = country!=null ? country.Id: (int?)null;
        regularSecurity.PricingFrequency = GetPricingFrequency(fileRow.ValFreq);
    }
    if (security is ShareClass shareClass)
    {
        shareClass.SubFundId = subfund != null? subfund.Id : (int?) null;
        // shareClass.NavFreq = (valFreq == 7310)? FrequencyType.Daily: throw new Exception("Nav frequency code unknown: "+ valFreq);
        shareClass.MinimumInvestment = fileRow.MinSubsAmt;
        shareClass.EntryFee = fileRow.FrontEndLoad;
        shareClass.ExitFee = fileRow.BackEndLoad;
        shareClass.IsOpenForInvestment = true;
        if (fileRow.SecName.Split(" - ").Length ==2 )
        {
            var extension = fileRow.SecName.Split(" - ")[1].ToLower();
            if (extension.Contains("dis"))
                shareClass.DividendDistributionPolicy = DividendDistributionPolicy.Distribution;
            if (extension.Contains("acc") || extension.Contains("cap"))
                shareClass.DividendDistributionPolicy = DividendDistributionPolicy.Accumulation;
        }
        
    }
    if (security is Bond bond)
    {
        bond.CouponRate = fileRow.IntrRate.HasValue? fileRow.IntrRate.Value/100 : (double?) null;
        bond.FaceValue = fileRow.FaceAmt.Value;
        bond.NextCouponDate = fileRow.NextCoupDate.Value;
        bond.MaturityDate = fileRow.MatRdmptDate.Value;
            
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
        option.Type = fileRow.EurAmStyle == "AMERICAN" ? OptionType.American : OptionType.European;
        option.PutCall = fileRow.SecName.Contains("put", System.StringComparison.InvariantCultureIgnoreCase) ? PutCall.Put : PutCall.Call;
    }
    if (security is StandardDerivative standardDerivative)
    {
        standardDerivative.StrikePrice = fileRow.StrikePrice;
        standardDerivative.ContractSize = fileRow.ContrSize;
    }
    return security;
}

string GetPortfolioInternalCode(string ContId)
    => ContId + "-BDL";

string GetMovementCode(string ContId, string OrderNr, string OrdTypId, double? NetAmount, DateTime bookDate)
    => ContId + "-" + OrderNr+ "-"
        + (string.IsNullOrEmpty(OrdTypId)? "" : OrdTypId)
        + ( NetAmount.HasValue? NetAmount+ "-" : "" )
        + bookDate.ToString("yyyy-MM-dd") 
        + "-BDL";

string getTransactionCode(string ContId, string OrderNr)
    => ContId + "-" + OrderNr   + "-BDL";

AccountType? GetCashAccountType(int cashType)
{
     switch (cashType)
    {
        case 9100:
           return AccountType.CurrentAccount;
        case 17027:
            return AccountType.DepositAccount;
        case 17063:
            return AccountType.GuaranteeDeposit;
        case 9130:
            return AccountType.MarginAccount;
        case 5674:
            return AccountType.CashClaimAccount;
        case 17706:
            return AccountType.FundAccounting;
        case 9120:
            return AccountType.PreciousMetals;
        default:
            throw new Exception($"Unknown cashType code: {cashType}");
    }
}
// <SecName>BlackRock Global World Gold - A2 EUR CAP</SecName>
// <Issuer>BlackRock Global Funds SICAV - World Gold Fund</Issuer>
// <SecName>Sycomore Selection Responsable - I CAP</SecName>
// <Issuer>Sycomore Selection Responsable FCP</Issuer>
// <SecName>Ethna Defensiv - T CAP</SecName>
// <Issuer>Ethna-DEFENSIV FCP</Issuer>

string GetSicavName(string issuerStr,string secName)
    => (!string.IsNullOrEmpty(issuerStr))? issuerStr.Split(" - ")[0]: secName.Split(" - ")[0];

string GetSubFundName(string issuerStr,string secName)
    => (!string.IsNullOrEmpty(issuerStr))? ((issuerStr.Split(" - ").Length >= 2) ? issuerStr.Split(" - ")[1] : issuerStr)
        : secName.Split(" - ")[0];

string GetIssuerInternalCode(string issuerStr,string secName,int instrType)
    => IsShareClassInstrType(instrType)? GetSicavName(issuerStr,secName): issuerStr;

string GetSecurityInternalCode(string isin, string securityCode)
    => (!string.IsNullOrEmpty(isin)) ? isin : securityCode;

SecurityTransaction CreateSecurityTransaction(BdlSectransNode FileRow,Portfolio Portfolio, Security Security1, Security Security2)
    => new SecurityTransaction
    {
    PortfolioId = Portfolio!=null? Portfolio.Id : throw new Exception("Saving Security Transaction - portfolio not found: "+FileRow.ContId+"-BDL"),
    SecurityId = (Security1!=null)? Security1.Id: ((Security2!=null)?
                    Security2.Id:throw new Exception($"Security not found {FileRow.SecurityCode}")),
    OperationType = FileRow.GrossAmount.Value <=0? OperationType.Buy: OperationType.Sale,
    TransactionCode = getTransactionCode(FileRow.ContId, FileRow.OrderNr),
    Description = FileRow.Communication1,
    TradeDate = FileRow.OrderDate,
    NavDate = FileRow.ExecDate,
    ValueDate = FileRow.ValDate,
    Quantity = Math.Abs(FileRow.SecQty),
    GrossAmountInSecurityCcy = Math.Abs(FileRow.GrossAmount.Value),
    GrossAmountInPortfolioCcy = (Math.Abs(FileRow.GrossAmount.Value) / (FileRow.Xrate.HasValue?FileRow.Xrate.Value:1.0)),
    NetAmountInPortfolioCcy = Math.Abs(FileRow.NetAmount.Value),
    NetAmountInSecurityCcy = (Math.Abs(FileRow.NetAmount.Value) * (FileRow.Xrate.HasValue?FileRow.Xrate.Value:1.0)),
    FeesInSecurityCcy = FileRow.BankFee.HasValue?FileRow.BankFee.Value:(double?)null,
    PriceInSecurityCcy = FileRow.ExecPrice,        
    TransactionType = TransactionType.SecurityMovement,
    DecisionType = TransactionDecisionType.Discretionary,
    //int? CashMovementId = string Iban {get;set;}// <Iban>LU870080399837602001</Iban> TODO: link with cash movement
    }; 

CashMovement CreateCashMovement(BdlCashtrNode FileRow,Currency Currency, Portfolio Portfolio, Cash Cash, SecurityTransaction SecTrans)
    => new CashMovement{
        PortfolioId = Portfolio.Id,
        CurrencyId = Currency.Id,
        TransactionCode = getTransactionCode(FileRow.ContId, FileRow.OrderNr),
        MovementCode = GetMovementCode(FileRow.ContId, FileRow.OrderNr, FileRow.OrdTypId, FileRow.NetAmount, FileRow.bookDate),
        TradeDate = FileRow.bookDate,
        ValueDate = FileRow.ValDate,
        ClosingDate = FileRow.ValDate,
        ExternalTransactionCode = FileRow.ExternalRef,
        CashId = (Cash !=null)? Cash.Id :(int?)null,
        TransactionId = (SecTrans != null)? SecTrans.Id:(int?)null,
        UnderlyingSecurityId = (SecTrans != null)? SecTrans.SecurityId:(int?)null,

        Description = FileRow.BookText + 
                    ((!string.IsNullOrEmpty(FileRow.Communication1))? " - " + FileRow.Communication1:"") +
                    ((!string.IsNullOrEmpty(FileRow.Communication2))? " - " + FileRow.Communication2:"") +
                    ((!string.IsNullOrEmpty(FileRow.Communication3))? " - " + FileRow.Communication3:"") +
                    ((!string.IsNullOrEmpty(FileRow.Communication4))? " - " + FileRow.Communication4:""),


        TransactionType = FileRow.BookText.ToLower().Contains("dividend")? TransactionType.Dividend
                        : FileRow.BookText.ToLower().Contains("coupon")? TransactionType.Coupon
                        : (FileRow.BookText.ToLower().Contains("subscription")||FileRow.BookText.ToLower().Contains("redemption"))?TransactionType.SubscriptionRedemption
                        : (FileRow.BookText.ToLower().Contains("buy")||FileRow.BookText.ToLower().Contains("sell"))?TransactionType.SecurityMovement
                        : (FileRow.BookText.ToLower().Contains("fee")||FileRow.BookText.ToLower().Contains("droit"))?TransactionType.ManagementFees
                        : (FileRow.BookText.ToLower().Contains("interest")||FileRow.BookText.ToLower().Contains("intérêt"))?TransactionType.Interest
                        : TransactionType.Cash,

        GrossAmountInSecurityCcy = FileRow.GrossAmt.HasValue? FileRow.GrossAmt.Value:(double?)null,
        GrossAmountInPortfolioCcy = FileRow.GrossAmt.HasValue? (FileRow.GrossAmt.Value / (FileRow.Xrate.HasValue?FileRow.Xrate.Value:1.0)):(double?)null,
        NetAmountInPortfolioCcy = FileRow.NetAmount.HasValue? FileRow.NetAmount.Value : (double?)null,
        NetAmountInSecurityCcy = FileRow.NetAmount.HasValue? (FileRow.NetAmount.Value * (FileRow.Xrate.HasValue?FileRow.Xrate.Value:1.0)): (double?)null,
        Reversal = FileRow.ReverseInd=="Y"? true : false,
        FeesInSecurityCcy =  (FileRow.GrossAmt.HasValue && FileRow.NetAmount.HasValue)? 
                            FileRow.GrossAmt.Value-(FileRow.NetAmount.Value * (FileRow.Xrate.HasValue?FileRow.Xrate.Value:1.0)): (double?)null,
        BrokerageFeesInSecurityCcy = FileRow.FeeComm,
    };
#endregion