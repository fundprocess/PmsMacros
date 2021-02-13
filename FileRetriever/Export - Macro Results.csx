
var macroExecutionsStream  = ProcessContextStream.EfCoreSelect($"{TaskName}: get LastMonitoringMacroExecution ", (ctx,j) => 
        ctx.Set<MonitoringMacroExecution>().Include(i=>i.MonitoringMacroCall)
        .Where(i => i.MonitoringMacroCall.Code == "NavEstimation")
        .GroupBy(i => i.MonitoringMacroCallId)
        .Select(i => new {MonitoringMacroCallId = i.Key, AsOfDate = i.Max(j => j.AsOfDate)})
        .Join(ctx.Set<MonitoringMacroExecution>()
                        .Include(i=>i.MonitoringMacroCall)
                        .Include(i=>i.Resultsets).ThenInclude(i => i.Related),
                i => i, i=> new {i.MonitoringMacroCallId,i.AsOfDate},(l,r) => r)
        );

var resultItemsStream = macroExecutionsStream
        .CrossApplyEnumerable($"{TaskName} Cross apply executions", i=> i.Resultsets);

var resultsStream = resultItemsStream
        .CrossApplyEnumerable($"{TaskName} Cross apply ResultSets", i=> GetResultItems(i.Result));


var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    Label = i.ToColumn<string>("Label"),
    Code = i.ToColumn<string>("Code"),
    //Date = i.ToOptionalDateColumn("Date", "yyyy-MM-dd"),
    Text = i.ToColumn<string>("Label"),
    
    // Number = i.ToNumberColumn<double>("Number", "."),
    // Integer = i.ToNumberColumn<double>("Integer", "."),
    // Percentage = i.ToNumberColumn<double>("Percentage", "."),
    
}).IsColumnSeparated(',');

var export = resultsStream
    .Select("create extract items", i => new {
        Label = i.Label,
        Code = i.Code,
        //Date = i.Date,
        Text = i.Text,
        // Number = i.Number,
        // Integer = i.Integer,
        // Percentage = i.Percentage,
    })
    .ToTextFileValue("Export to csv", $"ExportMacro - {DateTime.Today.ToString("yyyy-MM-dd")}.csv", csvFileDefinition);

return export;

List<MonitoringResult> GetResultItems(MonitoringResult result)
{
        var aa = new MonitoringResult
        {
                Label = "aaa",
                Code = "bbb",
                Text = "ccc",
        };
        return new List<MonitoringResult>(){aa};
        
        // if (result == null)
        //         throw new Exception("Result is null");
        // List<MonitoringResult> results = new List<MonitoringResult>();
        // results.Add(result);
        // if (result.Elements != null && result.Elements.Any())
        //         results.AddRange(result.Elements.SelectMany(i => GetResultItems(i)).ToList());
        // if (result.Rows != null && result.Rows.Any())
        //         results.AddRange(result.Rows.SelectMany(i => GetResultItems(i)).ToList());
        // if (result.Values != null && result.Values.Any())
        //         results.AddRange(result.Values.SelectMany(i => GetResultItems(i)).ToList());
        // return results;       
}

