
var deletedPortfolioStatistics = ProcessContextStream.EfCoreDelete($"{TaskName} delete Portfolio Statistics", o => o.Set<PortfolioStatistics>());
var deletedSecurityStatistics = ProcessContextStream.EfCoreDelete($"{TaskName} delete Security Statistics", o => o.Set<SecurityStatistics>());

return ProcessContextStream.WaitWhenDone("Wait for deletion", deletedPortfolioStatistics,deletedSecurityStatistics );
