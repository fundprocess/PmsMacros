string portfolioCode = "775400-UI";

var deleteBenchmarkCompos = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete Benchmark Compos", o => 
    o.Set<BenchmarkComposition>().Where((ctx,compo) => compo.Portfolio.InternalCode == "775400-UI" 
        && compo.Date > new DateTime(2020,08,31)));

var deletePortfolioCompos = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete MSCI Compos", o => 
    o.Set<PortfolioComposition>().Where((ctx,compo) => compo.Portfolio.InternalCode == "MSCI-Estimated"));

return ProcessContextStream.WaitWhenDone("Wait for deletion",deleteBenchmarkCompos,deletePortfolioCompos); 