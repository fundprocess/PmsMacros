// var dbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get dbStream", (ctx, j) => 
//         ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.BenchmarkComposition>().GroupBy(i=> i.PortfolioId)
//         .Select(i=> new {PortfolioId = i.Key, Date = i.Max(d=>d.Date) })
//         .Join(
//             ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.BenchmarkComposition>()
//                 .Include(i => i.Portfolio).ThenInclude(i => i.BenchmarkExposures)
//                 .Include(i => i.Positions).ThenInclude(i => (i as BenchmarkSecurityPosition).Security),
//             i => i,
//             i => new {i.PortfolioId, i.Date},
//             (l,r) => r)
//         );

var dbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get dbStream", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.BenchmarkComposition>()
            .Include(i => i.Portfolio).ThenInclude(i => i.BenchmarkExposures)
            .Include(i => i.Positions).ThenInclude(i => (i as BenchmarkSecurityPosition).Security)
            .Include(i => i.Positions).ThenInclude(i => ((i as BenchmarkSecurityPosition).Security as RegularSecurity).Country)
            .Include(i => i.Positions).ThenInclude(i => ((i as BenchmarkSecurityPosition).Security as SecurityInstrument).Classifications)
            .Include(i => i.Positions).ThenInclude(i => ((i as BenchmarkSecurityPosition).Security as SecurityInstrument).Classifications).ThenInclude(i=>i.ClassificationType)
            .Include(i => i.Positions).ThenInclude(i => ((i as BenchmarkSecurityPosition).Security as SecurityInstrument).Classifications).ThenInclude(i=>i.Classification)
            .OrderBy(i => i.Date)
        );

var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("PortfolioCode"),
    PortfolioName = i.ToColumn<string>("PortfolioName"),
    BenchmarkName = i.ToColumn<string>("BenchmarkName"),
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    SecurityCode = i.ToColumn<string>("SecurityCode"),
    SecurityName = i.ToColumn<string>("SecurityName"),
    Country = i.ToColumn<string>("Country"),
    GicsCode = i.ToColumn<string>("GicsCode"),
    GicsName = i.ToColumn<string>("GicsName"),
    Weight = i.ToNumberColumn<double>("Weight", "."),
}).IsColumnSeparated(',');

var gicsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get GICS", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Classifications.Classification>());

var export = dbStream.CrossApplyEnumerable($"{TaskName}: Cross-apply Positions", i=> i.Positions)
    .Select("create extract items", i => new {
        PortfolioCode = i.BenchmarkComposition.Portfolio.InternalCode,
        PortfolioName = i.BenchmarkComposition.Portfolio.Name,
        BenchmarkName = i.BenchmarkComposition.Portfolio.BenchmarkExposures.OrderByDescending(i => i.FromDate).FirstOrDefault()?.Name,
        Date = i.BenchmarkComposition.Date,
        SecurityCode = (i as BenchmarkSecurityPosition).Security.InternalCode,
        SecurityName = (i as BenchmarkSecurityPosition).Security.Name,
        Country  = ((i as BenchmarkSecurityPosition).Security as RegularSecurity).Country.Name["en"],
        GicsCode = ((i as BenchmarkSecurityPosition).Security as SecurityInstrument).Classifications.FirstOrDefault(i=>i.ClassificationType.Code.ToLower()=="gics")?.Classification.Code,
        GicsName = ((i as BenchmarkSecurityPosition).Security as SecurityInstrument).Classifications.FirstOrDefault(i=>i.ClassificationType.Code.ToLower()=="gics")?.Classification.Name["en"],
        Weight = i.Weight,
    })
    .ToTextFileValue("Export to csv", $"BenchmarkPositions - {DateTime.Today.ToString("yyyy-MM-dd")}.csv", csvFileDefinition);

return export;