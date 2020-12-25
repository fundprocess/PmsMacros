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

var securityStream = ProcessContextStream.EfCoreSelect($"{TaskName}: securities stream", 
                    (ctx, j) => ctx.Set<SecurityInstrument>());

var pricesStream = histValFileStream
    .Lookup($"{TaskName} get related security",securityStream, i => i.InternalCode, i => i.InternalCode,
        (l,r) => new {FileRow = l, Security= r} )
    .Where($"{TaskName}: where security not null", i => i.Security != null && i.FileRow.Value != null)
    .Select($"{TaskName}: create hist val", i=> new SecurityHistoricalValue
            {
                SecurityId = i.Security.Id,
                Date = i.FileRow.Date,
                Type = (HistoricalValueType)Enum.Parse(typeof(HistoricalValueType), i.FileRow.Type, true),
                Value = i.FileRow.Value.Value,
            })
    .Distinct($"{TaskName}: Distinct security prices", i => new {i.SecurityId,i.Type,i.Date})
    .EfCoreSave($"{TaskName}: Save security prices", o => o.SeekOn(i => new {i.SecurityId,i.Type,i.Date}).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is processed", pricesStream);
