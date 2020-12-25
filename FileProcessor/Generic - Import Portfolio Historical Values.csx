var fileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn("PortfolioCode"), 
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    Value = i.ToNumberColumn<double?>("Value", "."),
    Type =  i.ToColumn("Type"),
}).IsColumnSeparated(',');

var histValFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse historical values file", fileDefinition);
    //.SetForCorrelation($"{TaskName}: Set correlation key");

var portfoliosStream = ProcessContextStream.EfCoreSelect($"{TaskName}: portfolio stream", 
                    (ctx, j) => ctx.Set<Portfolio>());

var pricesStream = histValFileStream
    .Lookup($"{TaskName} get related portfolio",portfoliosStream, i => i.PortfolioCode, i => i.InternalCode,
        (l,r) => new {FileRow = l, Portfolio= r} )
    .Where($"{TaskName}: where portfolio not null", i => i.Portfolio != null && i.FileRow.Value != null)
    .Select($"{TaskName}: create hist val", i=> new PortfolioHistoricalValue
            {
                PortfolioId = i.Portfolio.Id,
                Date = i.FileRow.Date,
                Type = (HistoricalValueType)Enum.Parse(typeof(HistoricalValueType), i.FileRow.Type, true),
                Value = i.FileRow.Value.Value,
            })
    .Distinct($"{TaskName}: Distinct Portfolio prices", i => new {i.PortfolioId, i.Type, i.Date})
    .EfCoreSave($"{TaskName}: Save portfolio prices", o => o.SeekOn(i => new {i.PortfolioId, i.Type, i.Date}));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is processed", pricesStream);
