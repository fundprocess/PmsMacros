var dbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get dbStream", (ctx, j) => ctx
    .Set<SecurityHistoricalValue>().Include(i => i.Security));

var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"), 
    Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
    Value = i.ToNumberColumn<double>("Value", "."),
    Type =  i.ToColumn("Type"),
}).IsColumnSeparated(',');

var export = dbStream.Select("create extract items", i => new {
    InternalCode = i.Security.InternalCode,
    Date = i.Date,
    Value = i.Value,
    Type = i.Type.ToString(),
})
.ToTextFileValue("Export to csv", $"SecurityHistoricalValues - {DateTime.Today.ToString("yyyy-MM-dd")}.csv", csvFileDefinition);

return export;