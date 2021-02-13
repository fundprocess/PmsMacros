var securitiesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get securitiesStream", (ctx,j) => 
        ctx.Set<SecurityInstrument>()
        .Where(sec => !(sec is ShareClass) && (sec as ShareClass).SubFund.Sicav.IssuerId  != j.TenantId)) ;
        //.Where(sec => ctx.Set<Position>().Select(pos=>pos.SecurityId).Distinct().Contains(sec.Id)));

var deleteSecurityPrices = securitiesStream.EfCoreDelete($"{TaskName}: deleteSecurityPrices", 
    o => o.Set<SecurityHistoricalValue>().
    Where((sec, hv) => hv.SecurityId == sec.Id && hv.Type != HistoricalValueType.ESG ));

return ProcessContextStream.WaitWhenDone("Wait for deletion", deleteSecurityPrices);
