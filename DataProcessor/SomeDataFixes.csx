// //ADD A BLOOMBERG CODE
var shareClassStream = ProcessContextStream.EfCoreSelect($"{TaskName}: shareClassStream", i => i.Set<ShareClass>().Where(sc=>sc.InternalCode == "7754T1"));
var saveBbgCode = shareClassStream.Select($"{TaskName}: saveBbgCodeSelect", i => new SecurityDataProviderCode
    {
        Code = "UIUIVEU.LX",
        DataProvider = "Bloomberg",
        SecurityId = i.Id,
    })
    .EfCoreSave($"{TaskName}: saveBbgCodeSave", o => o.SeekOn(i => new { i.Code, i.SecurityId, i.DataProvider }));



//ADD NAV MANUALLY
// var shareClassStream = ProcessContextStream.EfCoreSelect($"{TaskName}: shareClassStream", i => i.Set<ShareClass>().Where(sc=>sc.InternalCode == "7754T1"));
// var saveNav = shareClassStream.Select($"{TaskName}: shareClassStreamSelect", i => new SecurityHistoricalValue
//     {
//         SecurityId = i.Id,
//         Date = new DateTime(2020,09,29),
//         Type = HistoricalValueType.MKT,
//         Value = 102.94
//     })
//     .EfCoreSave($"{TaskName}: shareClassStreamSave", o => o.SeekOn(i => new { i.Date, i.SecurityId, i.Type }));


//SET FUND DATA
// var primaryShareClassStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get shareclasses from db", i => i.Set<ShareClass>()
//                                 .Where(i=>i.InternalCode == "7754T1")).EnsureSingle($"{TaskName}: ensure one sc");
// var subFundStream = ProcessContextStream.EfCoreSelect("getsubFundStream", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 					.Select($"{TaskName}: link sc", primaryShareClassStream,(sf,sc)=>{
// 						sf.PrimaryShareClassId=sc.Id;
// 						sf.Url = "www.valuanalysis.com/the-fund/";
// 						sf.RecommendedTimeHorizon = 6;
// 						sf.InceptionDate = new DateTime(2020,01,29);
// 						sf.CutOffTime = new TimeSpan(13,30,0);
// 						sf.SettlementNbDays = 3;
// 						sf.InvestmentProcess = InvestmentProcessType.Qualitative;
// 						return sf;
// 					} ).EfCoreSave($"{TaskName}: save subfund",o=>o.WithMode(SaveMode.EntityFrameworkCore) );


//SET BENCHMARK
// var benchmarkStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get benchmark from db", i => i.Set<Benchmark>().Where(i=>i.InternalCode == "MXWO")).EnsureSingle($"{TaskName}: ensure one bench");
// var subFundStreamBench = ProcessContextStream.EfCoreSelect("getsubFundStreamBench", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 					.Select($"{TaskName}: link bench", benchmarkStream,(sf,bench)=>{
// 						sf.BenchmarkId=bench.Id;
// 						return sf;
// 					} ).EfCoreSave($"{TaskName}: save subfundBench");


//SET COUNTRY
// var lux = ProcessContextStream.EfCoreSelect($"{TaskName}: get country from db", i => i.Set<Country>().Where(i=>i.IsoCode2 == "LU")).EnsureSingle($"{TaskName}: ensure one country");
// var subFundStream2 = ProcessContextStream.EfCoreSelect("getsubFundStream2", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 					.Select($"{TaskName}: link sc2", lux,(sf,country)=>{
// 						sf.DomicileId=country.Id;
// 						sf.CountryId=country.Id;
// 						return sf;
// 					} ).EfCoreSave($"{TaskName}: save subfund2");

//SET STATIC DATA
// var setUrl  = ProcessContextStream.EfCoreSelect("setUrlSelect", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 						.Fix("setUrlFix", i => i.FixProperty(p => p.Url).AlwaysWith(p => "www.valuanalysis.com/the-fund/")).EfCoreSave("setUrlSave");
// var setTimeHorizon = ProcessContextStream.EfCoreSelect("Set Time horizon", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
//     .Fix("TH", i => i.FixProperty(p => p.RecommendedTimeHorizon).AlwaysWith(p => 7)).EfCoreSave("TH Save");



// return ProcessContextStream.WaitWhenDone("setTimeHorizon", setTimeHorizon);



//DELETE

// var ff = ProcessContextStream.EfCoreDelete("Delete", o => o.Set<ClassificationOfSecurity>().Where((ctx,i)=> i.ClassificationType.Code == "StoxxSector"));
// return ProcessContextStream.WaitWhenDone("Wait for deletion",ff);

// var deletePositions = ProcessContextStream.EfCoreDelete("Delete 1", o => o.Set<Position>().Where((ctx,i)=> i.PortfolioComposition.Portfolio.InternalCode == "775400"));
// var deleteCompo  = ProcessContextStream.EfCoreDelete("Delete composition", o => o.Set<PortfolioComposition>().Where((ctx,i)=> i.Portfolio.InternalCode == "775400"));
// var delete = ProcessContextStream.WaitWhenDone("wait composition deletion",deleteCompo).EfCoreDelete("Delete portfolio", o => o.Set<Portfolio>()
//                      .Where((ctx,i)=> i.InternalCode == "775400"));
// //var delete = ProcessContextStream.EfCoreDelete("Delete", o => o.Set<SubFund>().Where((ctx,i)=> i.InternalCode == "775400"));
// //return ProcessContextStream.WaitWhenDone("Wait for deletion",delete);
// return ProcessContextStream.WaitWhenDone("Wait for deletion",delete);

// var deleteCash = ProcessContextStream.EfCoreDelete("Delete CASH",(ProcessContext pc,Cash p) => p.Name.Contains("-TEST"));

// var deletedSicav = ProcessContextStream.WaitWhenDone($"{TaskName}: wait for portfolio deletion", deletedPortfolio)
//             .EfCoreDelete("Delete SICAV",(ProcessContext pc,Sicav s) => s.InternalCode == "UI I - ValuFocus");
// return ProcessContextStream.WaitWhenDone("Wait for SICAV deletion",deletedSicav);



