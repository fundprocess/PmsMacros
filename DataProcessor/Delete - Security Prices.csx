
// Delete security by security
// var securitiesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get securitiesStream", (ctx,j) => 
//         ctx.Set<SecurityInstrument>() 
//         .Where(sec => ctx.Set<Position>().Select(pos=>pos.SecurityId).Union(ctx.Set<BenchmarkSecurityPosition>().Select(pos=>pos.SecurityId)).Distinct().Contains(sec.Id)));

// var deleteSecurityPrices = securitiesStream.EfCoreDelete($"{TaskName}: deleteSecurityPrices", 
//     o => o.Set<SecurityHistoricalValue>().
//     Where((sec, hv) => hv.SecurityId == sec.Id && 
//              (hv.Type == HistoricalValueType.MKT || hv.Type == HistoricalValueType.TRP || hv.Type == HistoricalValueType.VOLU))); 
    
var deleteSecurityPrices = ProcessContextStream.EfCoreDelete($"{TaskName}: deleteSecurityPrices", 
    o => o.Set<SecurityHistoricalValue>().Where((i,j) => (j.Security is Equity) &&
             (j.Type == HistoricalValueType.MKT || j.Type == HistoricalValueType.TRP || j.Type == HistoricalValueType.VOLU)));

return ProcessContextStream.WaitWhenDone("Wait for deletion", deleteSecurityPrices);
