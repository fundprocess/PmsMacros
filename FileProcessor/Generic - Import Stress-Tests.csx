//Generic - Import Stress-tests

var fileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn("PortfolioCode"),
    ScenarioCode = i.ToColumn("ScenarioCode"),
    Scenario = i.ToColumn("Scenario"),
    Description = i.ToColumn("Description"),
    Date = i.ToDateColumn("Date" , "yyyy-MM-dd"),
    PortfolioImpact = i.ToNumberColumn<double?>("PortfolioImpact","."),
    BenchmarkImpact = i.ToNumberColumn<double?>("BenchmarkImpact","."),
}).IsColumnSeparated(',');

var stressTestFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse persons file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var scenariosDefinitionsStream = stressTestFileStream
    .Distinct($"{TaskName} Distinct Scenarios", i => i.ScenarioCode)
    .Select($"{TaskName} Create Scenarios", i => new StressTestScenario{
        Code = i.ScenarioCode,
        Name = new MultiCultureString { ["en"] = i.Scenario },
        Description  = new MultiCultureString { ["en"] = i.Description }
        //DateTime DateFrom { get; set; }
        //DateTime DateTo { get; set; }
    })
    .EfCoreSave($"{TaskName}: save scenarios", o => o.SeekOn(i => i.Code).DoNotUpdateIfExists());

var getLastComposStream = stressTestFileStream
    .Distinct($"{TaskName} Distinct Scenarios for get compo", i => i.PortfolioCode)
    .EfCoreSelect($"{TaskName} Get compos", (ctx,j) => 
        ctx.Set<PortfolioComposition>().Include(i=>i.Portfolio)
        .Where( compo => compo.Portfolio.InternalCode == j.Row.PortfolioCode && compo.Date == j.Row.Date));

var deleteExistingStream = getLastComposStream.EfCoreDelete($"{TaskName}: Delete existing st", o => o
        .Set<FundProcess.Pms.DataAccess.Schemas.RiskMgmt.PortfolioCompositionStressTestImpact>()
        .Where((i, j) => i.Id == j.PortfolioCompositionId ));

var scenariosImpactsStream = stressTestFileStream
    .WaitWhenDone($"{TaskName}: wait delete", deleteExistingStream)
    .CorrelateToSingle($"{TaskName} get related scenario", scenariosDefinitionsStream,
        (l,r) => new {FileRow = l, Scenario = r})
    .LookupPortfolio($"{TaskName} get related portfolio", i => i.FileRow.PortfolioCode,
        (l,r) => new {l.FileRow, l.Scenario, Portfolio = r})
    .Lookup($"{TaskName} get related compos", getLastComposStream, i => i.Portfolio.Id, i => i.PortfolioId,
        (l,r) => new {l.FileRow, l.Scenario, l.Portfolio, Composition = r})
    .Select($"{TaskName} Create scenarios impact", i => 
        new FundProcess.Pms.DataAccess.Schemas.RiskMgmt.PortfolioCompositionStressTestImpact{
        PortfolioCompositionId = i.Composition != null? i.Composition.Id
                        : throw new Exception("Portfolio composition not found :"+i.FileRow.PortfolioCode),
        ScenarioId = i.Scenario != null ? i.Scenario.Id
                        : throw new Exception("Scenario not found :"+i.FileRow.Scenario),
        Impact = i.FileRow.PortfolioImpact.HasValue ? i.FileRow.PortfolioImpact.Value
                    : throw new Exception("PortfolioImpact not set"),
        BenchmarkImpact = i.FileRow.BenchmarkImpact.HasValue ? i.FileRow.BenchmarkImpact.Value : (double?) null,
    })
    .EfCoreSave($"{TaskName}: save scenario impacts", o => o
        .SeekOn(i => new {i.PortfolioCompositionId, i.ScenarioId}));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", scenariosImpactsStream);