var fileDefinition = FlatFileDefinition.Create(i => new
{
    SecurityCode = i.ToColumn("SecurityCode"), 
    Name = i.ToColumn("Name"),
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    Price = i.ToNumberColumn<double?>("Price", "."),
    AdjustedPrice = i.ToNumberColumn<double?>("AdjustedPrice", "."),
    Volume = i.ToNumberColumn<double?>("Volume", "."),
}).IsColumnSeparated(',');


var pricesFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse historical values file", fileDefinition);
    //.SetForCorrelation($"{TaskName}: Set correlation key");

var securityStream = ProcessContextStream.EfCoreSelect($"{TaskName}: securities stream", 
                    (ctx, j) => ctx.Set<SecurityInstrument>());

var pricesStream = pricesFileStream
    .Lookup($"{TaskName} get related security",securityStream, i => i.SecurityCode, i => i.InternalCode,
        (l,r) => new {FileRow = l, Security= r} )
    .Where($"{TaskName}: where security not null", i => i.Security != null) 
    .CrossApplyEnumerable($"{TaskName}: Unpivot values",
        i => new[]
            {
                new { SecurityId = i.Security.Id,Date = i.FileRow.Date, Type = HistoricalValueType.MKT, Value = i.FileRow.Price },
                new { SecurityId = i.Security.Id,Date = i.FileRow.Date, Type = HistoricalValueType.TRP, Value = i.FileRow.AdjustedPrice },
                new { SecurityId = i.Security.Id,Date = i.FileRow.Date, Type = HistoricalValueType.VOLU, Value = i.FileRow.Volume },
            }
            .Where(j => j.Value != null)
            .Select(j => new SecurityHistoricalValue
            {
                SecurityId = j.SecurityId,
                Date = j.Date,
                Type = j.Type,
                Value = j.Value.Value,
            }))
    .Distinct($"{TaskName}: Distinct security prices", i => new {i.SecurityId,i.Type,i.Date})
    .EfCoreSave($"{TaskName}: Save security prices", o => o.SeekOn(i => new {i.SecurityId,i.Type,i.Date}));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is processed", pricesStream);
