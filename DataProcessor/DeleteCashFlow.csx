

var ff = ProcessContextStream.EfCoreDelete("Delete", o => o
    .Set<CashMovement>().Where((ctx,c)=>c.TradeDate >= DateTime.Today && c.Portfolio.InternalCode  == "6373008"));
return ProcessContextStream.WaitWhenDone("Wait for deletion",ff);
