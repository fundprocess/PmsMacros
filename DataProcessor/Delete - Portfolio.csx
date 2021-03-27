// //--------- Delete Security Prices ---------
// var securitiesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get securitiesStream", (ctx,j) => 
//         ctx.Set<SecurityInstrument>()
//         .Where(sec => ctx.Set<Position>().Select(pos=>pos.SecurityId).Distinct().Contains(sec.Id)));

// var deleteSecurityPrices = securitiesStream.EfCoreDelete($"{TaskName}: deleteSecurityPrices", 
//     o => o.Set<SecurityHistoricalValue>().
//     Where((sec, hv) => hv.SecurityId == sec.Id));

// //--------- Delete Index Prices ---------
// var indicesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get indicesStream", (ctx,j) => 
//         ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index>());

// var deleteIndexPrices = securitiesStream.EfCoreDelete($"{TaskName}: Delete deleteIndexPrices", o => 
//     o.Set<IndexHistoricalValue>().
//     Where((index, hv) => hv.IndexId == index.Id));

//--------- Delete Macro Executions ---------
// var deleteMacroExecutions = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete MacroExecutions", o => 
//     o.Set<MonitoringMacroExecution>().Where((i,j) => true) ); 

//--------- Delete Portfolio ---------
string portfolioCode = "SWDA";

var deletePortfolioCompos = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete Portfolio Compo", o => 
    o.Set<PortfolioComposition>().Where((ctx,compo) => compo.Portfolio.InternalCode == portfolioCode ) );

var deletePortfolio = ProcessContextStream
    .WaitWhenDone($"{TaskName} wait compo deletion",deletePortfolioCompos)
    .EfCoreDelete($"{TaskName}: Delete Portfolios", o => 
    o.Set<Portfolio>().Where((ctx,port) => port.InternalCode == portfolioCode ) ); 

return ProcessContextStream.WaitWhenDone("Wait for deletion" 
//,deleteSecurityPrices
//,deleteIndexPrices
//,deleteMacroExecutions
,deletePortfolio
);

