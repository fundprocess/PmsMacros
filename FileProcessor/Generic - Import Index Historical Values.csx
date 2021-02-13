var fileDefinition = FlatFileDefinition.Create(i => new
{
    //InternalCode	Date	Value	Type
    InternalCode = i.ToColumn("InternalCode"), 
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    Value = i.ToNumberColumn<double?>("Value", "."),
    Type =  i.ToColumn("Type"),
}).IsColumnSeparated(',');

var histValFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse historical values file", fileDefinition);
    //.SetForCorrelation($"{TaskName}: Set correlation key");

var indicesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: indices stream", 
                    (ctx, j) => ctx.Set<FundProcess.Pms.DataAccess.Schemas.Benchmarking.Index>());

var pricesStream = histValFileStream
    .Lookup($"{TaskName} get related index",indicesStream, i => i.InternalCode, i => i.InternalCode,
        (l,r) => new {FileRow = l, Index= r} )
    .Where($"{TaskName}: where Index not null", i => i.Index != null && i.FileRow.Value != null)
    .Select($"{TaskName}: create hist val", i=> new IndexHistoricalValue
            {
                IndexId = i.Index.Id,
                Date = i.FileRow.Date,
                Type = (HistoricalValueType)Enum.Parse(typeof(HistoricalValueType), i.FileRow.Type, true),
                Value = i.FileRow.Value.Value,
            })
    .Distinct($"{TaskName}: Distinct Index prices", i => new {i.Index,i.Type,i.Date})
    .EfCoreSave($"{TaskName}: Save Index prices", o => o.SeekOn(i => new {i.IndexId,i.Type,i.Date}).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is processed", pricesStream);
