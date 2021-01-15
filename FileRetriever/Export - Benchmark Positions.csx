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
                .OrderBy(i => i.Date)
                .Include(i => i.Portfolio).ThenInclude(i => i.BenchmarkExposures)
                .Include(i => i.Positions).ThenInclude(i => (i as BenchmarkSecurityPosition).Security));

var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("PortfolioCode"),
    PortfolioName = i.ToColumn<string>("PortfolioName"),
    BenchmarkName = i.ToColumn<string>("BenchmarkName"),
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    SecurityCode = i.ToColumn<string>("SecurityCode"),
    SecurityName = i.ToColumn<string>("SecurityName"),
    Weight = i.ToNumberColumn<double>("Weight", "."),
}).IsColumnSeparated(',');

var export = dbStream.CrossApplyEnumerable($"{TaskName}: Cross-apply Positions", i=> i.Positions)
            .Select("create extract items", i => new {
                PortfolioCode = i.BenchmarkComposition.Portfolio.InternalCode,
                PortfolioName = i.BenchmarkComposition.Portfolio.Name,
                BenchmarkName = i.BenchmarkComposition.Portfolio.BenchmarkExposures.OrderByDescending(i => i.FromDate).FirstOrDefault()?.Name,
                Date = i.BenchmarkComposition.Date,
                SecurityCode = (i as BenchmarkSecurityPosition).Security.InternalCode,
                SecurityName = (i as BenchmarkSecurityPosition).Security.Name,
                Weight = i.Weight,
            })
            .ToTextFileValue("Export to csv", $"BenchmarkPositions - {DateTime.Today.ToString("yyyy-MM-dd")}.csv", csvFileDefinition);

return export;