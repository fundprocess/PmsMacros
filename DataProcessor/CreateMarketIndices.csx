//Important notice: set to !! OVERWRITE !!

//From EOD support desk, regarding Risk Free:
//  EURIBOR, we have 1 week, 1 month, 3 months, 6 months and 12 months rates. To get, for example, 3 months rate for EURIBOR, 
//  use the following ticker: EURIBOR3M.MONEY
//  For LIBOR we have 1 week, 1 month, 2 months, 3 months, 6 months and 12 months rates nominated in four different 
//  currencies: USD, EUR, GBP and JPY. To get, for example, 2 months rate for LIBOR nominated in EURO, 
//  use the following ticker: LIBOREUR2M.MONEY

var indicesInputStream = new List<IndexTmp>{

new IndexTmp{ InternalCode = "MIWO0TC00NUS",Name = "MSCI World Telecommunications Srvcs Net USD", CcyIso="USD", EodCode = "MIWO0TC00NUS.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWCONSDISC",Name = "MSCI World Consumer Discretionary", CcyIso="USD", EodCode = "MSCIWCONSDISC.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWCONSSTAP",Name = "MSCI World Consumer Staples", CcyIso="USD", EodCode = "MSCIWCONSSTAP.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWENERGY",Name = "MSCI World Energy", CcyIso="USD", EodCode = "MSCIWENERGY.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWFIN",Name = "MSCI World Financials", CcyIso="USD", EodCode = "MSCIWFIN.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWHEALTH",Name = "MSC World Healthcare", CcyIso="USD", EodCode = "MSCIWHEALTH.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWIND",Name = "MSCI World Industrials", CcyIso="USD", EodCode = "MSCIWIND.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWINFTECH",Name = "MSCI World Information Technologie", CcyIso="USD", EodCode = "MSCIWINFTECH.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWMAT",Name = "MSCI World Materials", CcyIso="USD", EodCode = "MSCIWMAT.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIWUTIL",Name = "MSCI World Utilities", CcyIso="USD", EodCode = "MSCIWUTIL.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "VX",Name = "S&P 500 VIX Futures", CcyIso="USD", EodCode = "VX.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "NQ",Name = "NASDAQ 100", CcyIso="USD", EodCode = "NQ.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "STOXX",Name = "Europe Stoxx 600", CcyIso="EUR", EodCode = "STOXX.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIEU",Name = "MSCI Europe", CcyIso="EUR", EodCode = "MSCIEU.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MSCIUSA",Name = "MSCI USA", CcyIso="USD", EodCode = "MSCIUSA.INDX", BloombergCode=""},
new IndexTmp{ InternalCode = "MIJP00000NUS",Name = "MSCI Japan Net USD", CcyIso="USD", EodCode = "MIJP00000NUS.INDX", BloombergCode=""},

// new IndexTmp{ InternalCode = "MSCIWORLD" , Name = "MSCI World" , CcyIso ="USD", EodCode = "MSCIWORLD.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "MSCIWORLDNETEUR" , Name = "MSCI World Net EUR" , CcyIso ="EUR", EodCode = "MSCIWORLDNETEUR.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "MSCIWORLDNETUSD" , Name = "MSCI World Net USD" , CcyIso ="USD", EodCode = "MSCIWORLDNETUSD.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "SX5E" , Name = "Euro Stoxx 50 Pr" , CcyIso ="EUR", EodCode = "SX5E.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "3LHE" , Name = "ESTX 50 CORPORATE BOND TR" , CcyIso ="EUR", EodCode = "3LHE.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "SP500BDT" , Name = "S&P 500 Bond Index" , CcyIso ="USD", EodCode = "SP500BDT.INDX", BloombergCode = "",CountryIso2="US"},
// new IndexTmp{ InternalCode = "SPXNTR" , Name = "S&P 500 Net TR" , CcyIso ="USD", EodCode = "SPXNTR.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "MSCIEF" , Name = "MSCI Emerging Markets" , CcyIso ="USD", EodCode = "MSCIEF.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "FCHI" , Name = "CAC 40" , CcyIso ="EUR", EodCode = "FCHI.INDX", BloombergCode = "", CountryIso2="FR"},
// new IndexTmp{ InternalCode = "JPXNK400" , Name = "JPX-Nikkei 400" , CcyIso ="JPY", EodCode = "JPXNK400.INDX", BloombergCode = "", CountryIso2="JP"},
// new IndexTmp{ InternalCode = "NDX" , Name = "Nasdaq 100" , CcyIso ="USD", EodCode = "NDX.INDX", BloombergCode = "", CountryIso2="US"},
// new IndexTmp{ InternalCode = "EUU" , Name = "Euro Currency Index" , CcyIso = "EUR", EodCode = "EUU.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "FTXIN9" , Name = "FTSE China A50" , CcyIso ="CNY", EodCode = "FTXIN9.INDX", BloombergCode = "", CountryIso2="CN"},
// new IndexTmp{ InternalCode = "AW07" , Name = "FTSE ASIA PACIFIC EX JAPAN INDEX" , CcyIso ="USD", EodCode = "AW07.INDX", BloombergCode = ""},
// new IndexTmp{ InternalCode = "GSPTSE" , Name = "S&P TSX Composite Index (Canada)" , CcyIso ="CAD", EodCode = "GSPTSE.INDX", BloombergCode = "", CountryIso2="CA"},
// new IndexTmp{ InternalCode = "AORD" , Name = "Australia All Ordinaries" , CcyIso ="AUD", EodCode = "AORD.INDX", BloombergCode = "", CountryIso2="AU"},
// new IndexTmp{ InternalCode = "IBX50" , Name = "Bovespa Brazil 50" , CcyIso ="BRL", EodCode = "IBX50.INDX", BloombergCode = "", CountryIso2="BR"},
// new IndexTmp{ InternalCode = "BEL20" , Name = "BEL-20 INDEX" , CcyIso = "EUR", EodCode = "BEL20.INDX", BloombergCode = "", CountryIso2="BE"},
// //new IndexTmp{ InternalCode = "DBDCONIA INDEX" , Name ="Eonia Capitalised Index (EUR)" , CcyIso ="EUR", EodCode = "", BloombergCode="DBDCONIA INDEX" ,IsCurrencyRiskFree=true},
// new IndexTmp{ InternalCode = "VIX", Name = "CBOE Volatility Index" , CcyIso ="", EodCode = "VIX.INDX", BloombergCode = "" , CountryIso2="US"},

// new IndexTmp{ InternalCode = "BIL.US", Name = "SPDR Bloomberg Barclays 1-3 Month T-Bill ETF" , CcyIso ="USD", EodCode = "BIL.US", 
//                 BloombergCode = "" , CountryIso2="US"},

// //RISK FREE INDICES:
// new IndexTmp{ InternalCode = "Euribor3M", Name = "Euribor 3-Months" , CcyIso ="", EodCode = "EURIBOR3M.MONEY", BloombergCode = "" , 
//             CountryIso2="", IsRateIndex = true, DayCountConvention=360},

// //new IndexTmp{ InternalCode = "LiborEUR3M", Name = "Libor EUR 3-Months" , CcyIso ="", EodCode = "LIBOREUR3M.MONEY", BloombergCode = "" , CountryIso2=""},
// // new IndexTmp{ InternalCode = "LiborUSD3M", Name = "Libor USD 3-Months" , CcyIso ="", EodCode = "LIBORUSD3M.MONEY", BloombergCode = "" , 
// //             CountryIso2="",IsRateIndex = true, DayCountConvention=360},
// // new IndexTmp{ InternalCode = "LiborGBP3M", Name = "Libor GBP 3-Months" , CcyIso ="", EodCode = "LIBORGBP3M.MONEY", BloombergCode = "" , CountryIso2="",
// //             IsRateIndex = true, DayCountConvention=360},
// // new IndexTmp{ InternalCode = "LiborJPY3M", Name = "Libor JPY 3-Months" , CcyIso ="", EodCode = "LIBORJPY3M.MONEY", BloombergCode = "" , CountryIso2="",
// //             IsRateIndex = true, DayCountConvention=360},

// //--------------- US T-BILL ---------------
// // new IndexTmp{ InternalCode = "US1M.GBOND", Name = "U.S. 1 Month Treasury Bill" , CcyIso ="", EodCode = "US1M.GBOND", BloombergCode = "" , CountryIso2="US"},
// new IndexTmp{ InternalCode = "US3M.GBOND", Name = "U.S. 3 Month Treasury Bill" , CcyIso ="", EodCode = "US3M.GBOND", BloombergCode = "" , CountryIso2="US"
//                 , IsRateIndex = true, DayCountConvention=360},
// // new IndexTmp{ InternalCode = "US6M.GBOND", Name = "U.S. 6 Month Treasury Bill" , CcyIso ="", EodCode = "US6M.GBOND", BloombergCode = "" , CountryIso2="US"},
// // new IndexTmp{ InternalCode = "US1Y.GBOND", Name = "U.S. 1 Year Treasury Bill" , CcyIso ="", EodCode = "US1Y.GBOND", BloombergCode = "" , CountryIso2="US"},
// // new IndexTmp{ InternalCode = "US2Y.GBOND", Name = "U.S. 2 Year Treasury Bill" , CcyIso ="", EodCode = "US2Y.GBOND", BloombergCode = "" , CountryIso2="US"},
// // new IndexTmp{ InternalCode = "US5Y.GBOND", Name = "U.S. 5 Year Treasury Bill" , CcyIso ="", EodCode = "US5Y.GBOND", BloombergCode = "" , CountryIso2="US"},
// // new IndexTmp{ InternalCode = "US7Y.GBOND", Name = "U.S. 7 Year Treasury Bill" , CcyIso ="", EodCode = "US7Y.GBOND", BloombergCode = "" , CountryIso2="US"},
// // new IndexTmp{ InternalCode = "US10Y.GBOND", Name = "U.S. 10 Year Treasury Bill" , CcyIso ="", EodCode = "US10Y.GBOND", BloombergCode = "" , CountryIso2="US"},
// // new IndexTmp{ InternalCode = "US30Y.GBOND", Name = "U.S. 30 Year Treasury Bill" , CcyIso ="", EodCode = "US30Y.GBOND", BloombergCode = "" , CountryIso2="US"},
// //new IndexTmp{ InternalCode = "DE10Y.GBOND", Name = "Germany 10-Year Bond Yield" , CcyIso ="", EodCode = "DE10Y.GBOND", BloombergCode = "" , CountryIso2="DE"},
};

var indexStream = ProcessContextStream.
        CrossApplyEnumerable($"{TaskName}: Cross apply input", i=> indicesInputStream)
        .LookupCurrency($"{TaskName}: Get releated currency", i => i.CcyIso, 
                (l,r) => new { Row = l, Currency = r})
        .LookupCountry($"{TaskName}: Get releated country", i => i.Row.CountryIso2, 
                (l,r) => new { Row = l.Row, Currency = l.Currency, Country = r}  )
        .Select($"{TaskName}: Create Indices",i=> new { 
                EodCode = i.Row.EodCode,
                BloombergCode = i.Row.BloombergCode,
                Index = new FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index{
                    InternalCode = i.Row.InternalCode,
                    Name = i.Row.Name,
                    ShortName = i.Row.InternalCode,
                    PricingFrequency = FrequencyType.Daily,
                    IsCurrencyRiskFree = i.Row.IsCurrencyRiskFree,
                    ReferenceCurrencyId = i.Currency != null? i.Currency.Id : (int?) null,
                    ReferenceCountryId  = i.Country != null? i.Country.Id : (int?) null,
                    IsRateValues = i.Row.IsRateIndex,
                    DayCountConvention = i.Row.DayCountConvention,

        }})
        .EfCoreSave("Save Indices", o=>o.Entity(i => i.Index).SeekOn(i => i.InternalCode).DoNotUpdateIfExists().Output((i,e)=>i)); 
           
var saveBbgCode = indexStream
    .Where($"{TaskName}: where bloomberg code", i => !string.IsNullOrEmpty(i.BloombergCode))
    .Select($"{TaskName}: save bbg code", i => new IndexDataProviderCode
    {
        IndexId = i.Index.Id,
        Code = i.BloombergCode,
        DataProvider = "Bloomberg",
    })
    .EfCoreSave($"{TaskName}: saveBbgCode", o => o.SeekOn(i => new { i.IndexId, i.DataProvider }));

var saveEodCode = indexStream
    .Where($"{TaskName}: where EOD code", i => !string.IsNullOrEmpty(i.EodCode))
    .Select($"{TaskName}: save eod code", i => new IndexDataProviderCode
    {
        IndexId = i.Index.Id,
        Code = i.EodCode,
        DataProvider = "EOD",
    })
    .EfCoreSave($"{TaskName}: save EOD Code", o => o.SeekOn(i => new { i.IndexId, i.DataProvider }));

ProcessContextStream.WaitWhenDone("wait till everything is done",saveBbgCode,saveEodCode);

public class IndexTmp
{
        public string InternalCode {get;set;}
        public string Name {get;set;}
        public string CcyIso {get;set;}
        public string EodCode {get;set;}
        public string BloombergCode {get;set;}
        public string CountryIso2 {get;set;}
        public bool IsCurrencyRiskFree {get;set;}
        public bool IsRateIndex {get;set;}
        public int? DayCountConvention {get;set;}

}