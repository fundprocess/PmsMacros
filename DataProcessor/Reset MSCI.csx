string portfolioCode = "775400-UI";

var deleteBenchmarkCompos = ProcessContextStream.EfCoreDelete($"{TaskName}: Delete Benchmark Compos", o => 
    o.Set<BenchmarkComposition>().Where((ctx,compo) => compo.Portfolio.InternalCode == portfolioCode 
        && compo.Date > new DateTime(2020,08,31)));

return ProcessContextStream.WaitWhenDone("Wait for deletion",deleteBenchmarkCompos); 