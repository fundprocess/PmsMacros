// var deletePortfolioStatistics = ProcessContextStream.EfCoreDelete($"{TaskName} delete Portfolio Statistics", o => o.Set<PortfolioStatistics>());
// var deleteSecurityStatistics = ProcessContextStream.EfCoreDelete($"{TaskName} delete Security Statistics", o => o.Set<SecurityStatistics>());

var deleteBenchmarkPositions = ProcessContextStream.EfCoreDelete($"{TaskName} delete Benchmark Positions", o => o.Set<BenchmarkPosition>());
return ProcessContextStream.WaitWhenDone("Wait for deletion",deleteBenchmarkPositions );
