#region Prices

var deleteIndexPrices = ProcessContextStream.EfCoreDelete($"{TaskName} deleteIndexPrices", o => o.Set<IndexHistoricalValue>()
        .Where((ctx,i)=> i.Date == new DateTime(2020,12,31) && i.Index.InternalCode == "MSCIWORLDNETUSD"
        && (i.Type == HistoricalValueType.MKT || i.Type == HistoricalValueType.TRP)));

#endregion

#region Cash Issuer
var cashSecuritiesWithNoIssuer = ProcessContextStream.EfCoreSelect($"{TaskName}: get cash securities", (ctx, j) => 
                        ctx.Set<Cash>().Where(i => !i.IssuerId.HasValue));

var bbh = ProcessContextStream.EfCoreSelect($"{TaskName}: get issuer entity", (ctx, j) => 
                        ctx.Set<Company>().Where(i => i.InternalCode == "bbh"))
                        .EnsureSingle($"Ensure one");

var saveCashIssuers = cashSecuritiesWithNoIssuer.Select($"{TaskName}: set issuer",bbh, (i,bbh) => {
         i.IssuerId = (bbh != null)? bbh.Id : throw new Exception("Cash issuer not found");
         return i;
     })
     .EfCoreSave($"{TaskName}: save cash");
#endregion

return ProcessContextStream.WaitWhenDone("Wait done",saveCashIssuers,deleteIndexPrices);


#region Fix primary shareclass
// var primaryShareClassStream = shareClassStream
//         .Sort($"{TaskName}: Sort share classes by AUM desc", 
//             i => new {i.ShareClass.SubFundId.Value,i.TotalNetAsset}, new {FundCode = 1,TotalNetAsset = -2})
//         .Distinct($"{TaskName}: Distinct AUM sorted share classes", i => i.ShareClass.SubFundId); //take the shareclass id that comes first at it has the biggest aum for the subfund

// var subFundsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get sub funds Stream", (ctx, j) => 
//     ctx.Set<SubFund>().Include(i => i.Sicav).Where(i=>i.Sicav.IssuerId == j.TenantId));

// var subfundsPrimaryShareClassStream = subfundsStream
//     .Lookup($"{TaskName}: get related primary share class", primaryShareClassStream, i => i.Id, i => i.ShareClass.SubFundId.Value,
//         (l,r) => new { sf = l, sc = r})
//     .Lookup($"{TaskName}: get related sub fund db", subFundsStreamDb, i => i.sf.Id, i => i.Id,
//         (l,r) => new { sf = l.sf, sc = l.sc, sfdb = r})
//     .Select($"{TaskName}: set primary share class", i => {
//         i.sfdb.PrimaryShareClassId = (i.sc != null)? i.sc.ShareClass.Id : throw new Exception("Primary share class null");
//         return i.sfdb;
//     })
//     .EfCoreSave($"{TaskName}: save sf");

//---------------------
// var primaryShareClassStream=shareClassStream
//         .Sort($"{TaskName}: Sort share classes by AUM desc", i => new {i.ShareClass.SubFundId,i.TotalNetAsset}, new {FundCode = 1,TotalNetAsset = -2})
//         .Distinct($"{TaskName}: Distinct AUM sorted share classes", i => i.ShareClass.SubFundId); //take the shareclass id that comes first at it has the biggest aum for the subfund

// var subfundsPrimaryShareClassStream = subfundsStream
//     .CorrelateToSingle($"{TaskName}: get related primary share class", primaryShareClassStream,(sf, sc)=> {
//         sf.PrimaryShareClassId = (sc != null)? sc.ShareClass.Id : throw new Exception("Primary share class not found for" + sf.Name);
//         return sf;
//     })
//     .EfCoreSave($"{TaskName}: save sf");
#endregion Fix primary shareclass


#region complete role when missing:  investor advisors (primary & secondary) and intermediary
// string primaryAdvisorCode="";
// string secondaryAdvisorCode="";
// string intermediaryCode="";
// var primaryAdvisor = ProcessContextStream.EfCoreSelect($"{TaskName}: get primary advisor from db", (ctx, j) => 
//                         ctx.Set<RoleRelationship>().Include(i=>i.Entity).Include(i=>i.Role)
//                         .Where(i=>i.Entity.InternalCode == primaryAdvisorCode && 
//                             i.Role.Domain == RoleDomain.ClientAdvisor)).EnsureSingle($"Ensure primary client advisor");
// var fixPrimaryAdvisor = ProcessContextStream
//         .EfCoreSelect($"{TaskName}: get investor from db", 
//                         (ctx, j) => ctx.Set<InvestorRelationship>().Where(i=> !i.PrimaryInternalAdvisorId.HasValue))
//         .Select($"{TaskName}: link primaryAdvisor", primaryAdvisor, (investor,advisor)=>{
//                 investor.PrimaryInternalAdvisorId = advisor!=null? advisor.Id :throw new Exception("primary advisor null");
//                 return investor;
//         })
//         .EfCoreSave($"{TaskName}: save primary advisor");

// var secondaryAdvisor = ProcessContextStream.EfCoreSelect($"{TaskName}: get secondary advisor from db", (ctx, j) => 
//                         ctx.Set<RoleRelationship>().Include(i=>i.Entity).Include(i=>i.Role)
//                         .Where(i=>i.Entity.InternalCode == secondaryAdvisorCode
//                         && i.Role.Domain == RoleDomain.ClientAdvisor)).EnsureSingle($"Ensure secondary client advisor");

// var fixSecondaryAdvisor = ProcessContextStream
//         .EfCoreSelect($"{TaskName}: get investor 2 from db", 
//                 (ctx, j) => ctx.Set<InvestorRelationship>().Where(i=> !i.SecondaryInternalAdvisorId.HasValue))
//         .Select($"{TaskName}: link secondary Advisor", secondaryAdvisor, (investor,advisor)=>{
//                 investor.SecondaryInternalAdvisorId =  advisor!=null? advisor.Id :throw new Exception("advisor null");
//                 return investor;
//         })
//         .EfCoreSave($"{TaskName}: save secondary advisor");
// var intermediary = ProcessContextStream.EfCoreSelect($"{TaskName}: get intermediary relationship", (ctx, j) => 
//                         ctx.Set<RoleRelationship>().Include(i=>i.Entity).Include(i=>i.Role)
//                         .Where(i=>i.Entity.InternalCode == intermediaryCode
//                         && i.Role.Code == "Distributor")).EnsureSingle($"Ensure Intermediary");

// var fixIntermediary = ProcessContextStream
//         .EfCoreSelect($"{TaskName}: get intermediary from db", 
//                 (ctx, j) => ctx.Set<InvestorRelationship>().Where(i=> !i.IntermediaryId.HasValue))
//         .Select($"{TaskName}: link intermediary", intermediary, (investor,intermediary)=>{
//                 investor.IntermediaryId = intermediary.Id;
//                 return investor;
//         })
//         .EfCoreSave($"{TaskName}: save intermediary");

//return ProcessContextStream.WaitWhenDone("Wait done",fixPrimaryAdvisor,fixSecondaryAdvisor,fixIntermediary);

#endregion



#region Fix Statistics set to compute
// var subFundsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get sub funds Stream", (ctx, j) => 
//         ctx.Set<SubFund>().Include(i => i.Sicav).Where(i=>i.Sicav.IssuerId == j.TenantId));

// var statisticsSet = ProcessContextStream.EfCoreSelect($"{TaskName}: get statistics set", (ctx, j) => 
//         ctx.Set<StatisticDefinitionSet>().Where(i => i.Code == "DailyStatistics"))
//         .EnsureSingle($"{TaskName}: ensure single daily statistics set");

// var savePortfolioStatisticsSet = subFundsStream
//         .Select($"{TaskName} create PortfolioStatisticDefinitionSet",statisticsSet, (i,j) =>
//         new PortfolioStatisticDefinitionSet{
//                 PortfolioId = i.Id,
//                 StatisticDefinitionSetId = j.Id,
//         })
//         .EfCoreSave($"{TaskName}: Save PortfolioStatisticDefinitionSet", o => o
//         .SeekOn(i => new {i.PortfolioId, i.StatisticDefinitionSetId}).DoNotUpdateIfExists());

// return ProcessContextStream.WaitWhenDone("Wait done", savePortfolioStatisticsSet);

#endregion Fix Statistics set to compute




// //ADD A BLOOMBERG CODE
// var shareClassStream = ProcessContextStream.EfCoreSelect($"{TaskName}: shareClassStream", 
                //i => i.Set<ShareClass>().Where(sc=>sc.InternalCode == "7754T1"));
// var saveBbgCode = shareClassStream.Select($"{TaskName}: saveBbgCodeSelect", i => new SecurityDataProviderCode
//     {
//         Code = "***.LX",
//         DataProvider = "Bloomberg",
//         SecurityId = i.Id,
//     })
//     .EfCoreSave($"{TaskName}: saveBbgCodeSave", o => o.SeekOn(i => new { i.Code, i.SecurityId, i.DataProvider }));

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
// var subFundStream = ProcessContextStream.EfCoreSelect($"{TaskName}: getsubFundStream", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
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
// var subFundStreamBench = ProcessContextStream.EfCoreSelect($"{TaskName}: getsubFundStreamBench", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 					.Select($"{TaskName}: link bench", benchmarkStream,(sf,bench)=>{
// 						sf.BenchmarkId=bench.Id;
// 						return sf;
// 					} ).EfCoreSave($"{TaskName}: save subfundBench");


//SET COUNTRY
// var lux = ProcessContextStream.EfCoreSelect($"{TaskName}: get country from db", i => i.Set<Country>().Where(i=>i.IsoCode2 == "LU")).EnsureSingle($"{TaskName}: ensure one country");
// var subFundStream2 = ProcessContextStream.EfCoreSelect($"{TaskName}: getsubFundStream2", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 					.Select($"{TaskName}: link sc2", lux,(sf,country)=>{
// 						sf.DomicileId=country.Id;
// 						sf.CountryId=country.Id;
// 						return sf;
// 					} ).EfCoreSave($"{TaskName}: save subfund2");

//SET STATIC DATA
// var setUrl  = ProcessContextStream.EfCoreSelect($"{TaskName}: setUrlSelect", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
// 						.Fix("setUrlFix", i => i.FixProperty(p => p.Url).AlwaysWith(p => "www.valuanalysis.com/the-fund/")).EfCoreSave("setUrlSave");
// var setTimeHorizon = ProcessContextStream.EfCoreSelect($"{TaskName}: Set Time horizon", i => i.Set<SubFund>().Where(p => p.InternalCode == "7754"))
//     .Fix("TH", i => i.FixProperty(p => p.RecommendedTimeHorizon).AlwaysWith(p => 7)).EfCoreSave("TH Save");



// return ProcessContextStream.WaitWhenDone("setTimeHorizon", setTimeHorizon);



//DELETE

// var ff = ProcessContextStream.EfCoreDelete("Delete", o => o.Set<ClassificationOfSecurity>().Where((ctx,i)=> i.ClassificationType.Code == "StoxxSector"));
// return ProcessContextStream.WaitWhenDone("Wait for deletion",ff);

// var deletePositions = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete 1", o => o.Set<Position>().Where((ctx,i)=> i.PortfolioComposition.Portfolio.InternalCode == "775400"));
// var deleteCompo  = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete composition", o => o.Set<PortfolioComposition>().Where((ctx,i)=> i.Portfolio.InternalCode == "775400"));
// var delete = ProcessContextStream.WaitWhenDone($"{TaskName}: wait composition deletion",deleteCompo).EfCoreDelete("Delete portfolio", o => o.Set<Portfolio>()
//                      .Where((ctx,i)=> i.InternalCode == "775400"));
// //var delete = ProcessContextStream.EfCoreDelete("Delete", o => o.Set<SubFund>().Where((ctx,i)=> i.InternalCode == "775400"));
// //return ProcessContextStream.WaitWhenDone($"{TaskName}: Wait for deletion",delete);
// return ProcessContextStream.WaitWhenDone($"{TaskName}: Wait for deletion",delete);

// var deleteCash = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete CASH",(ProcessContext pc,Cash p) => p.Name.Contains("-TEST"));

// var deletedSicav = ProcessContextStream.WaitWhenDone($"{TaskName}: wait for portfolio deletion", deletedPortfolio)
//             .EfCoreDelete($"{TaskName}: Delete SICAV",(ProcessContext pc,Sicav s) => s.InternalCode == "UI I - ValuFocus");
// return ProcessContextStream.WaitWhenDone($"{TaskName}: Wait for SICAV deletion",deletedSicav);
