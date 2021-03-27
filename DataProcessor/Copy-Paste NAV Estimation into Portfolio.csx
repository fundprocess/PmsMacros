

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

var estimatedShareClassHvsStream = resultItemsStream
        .CrossApplyEnumerable($"{TaskName} Cross apply ResultSets from Nav Estimated", i=> GetEstimatedNavs(i.Result))
        .EfCoreSave($"{TaskName} Save share class hv",  o => o.SeekOn(i => new {i.SecurityId,i.Date,i.Type}).DoNotUpdateIfExists());

var estimatedPortfolioTnasStream = resultItemsStream
        .CrossApplyEnumerable($"{TaskName} Cross apply ResultSets from TNA Estimated", i=> GetEstimatedTnas(i.Result))
        .EfCoreSave($"{TaskName} Save portfolio Tnas",  o => o.SeekOn(i => new {i.PortfolioId,i.Date,i.Type}).DoNotUpdateIfExists());

var newCompositionsStream = resultItemsStream
        .Select($"{TaskName} Cross apply ResultSets for compo", i=> GetEstimatedComposition(i.Result));

var portfoliosStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get portfolios stream", (ctx,j) => 
        ctx.Set<Portfolio>());

var newCompoSaved = newCompositionsStream        
        .Lookup($"{TaskName} Get Related Portfolio",portfoliosStream, i=>i.PortfolioCode, i=> i.InternalCode,
                (l,r) => new {Row = l , Portfolio = r})
        .Select($"{TaskName} Create new composition",i => new PortfolioComposition
        {
                PortfolioId = (i.Portfolio != null)? i.Portfolio.Id : throw new Exception("Portfolio not found: " + i.Row.PortfolioCode) ,
                Date = i.Row.Date
        })
        .EfCoreDelete($"{TaskName} Delete current positions", o=>o.Set<Position>().Where((composition, position) 
                => position.PortfolioComposition.Date == composition.Date && position.PortfolioComposition.PortfolioId == composition.PortfolioId ))
        .EfCoreSave($"{TaskName} Save composition",  o => o.SeekOn(i => new {i.PortfolioId,i.Date}).DoNotUpdateIfExists())
        .EnsureSingle($"{TaskName} ensure single");

var newPositionsSaved = newCompositionsStream.CrossApplyEnumerable($"{TaskName} CrossApplyEnumerable positions",i => i.Positions)    
        .Lookup($"{TaskName} Get Related Compo", newCompoSaved, i=> i.Date, i => i.Date,
                (l,r) => new { Position = l, Composition = r})
        .Select($"{TaskName} Create new positions", i=> new Position()
        {
                PortfolioCompositionId = (i.Composition != null)? i.Composition.Id : throw new Exception("Composition not found"),
                SecurityId = i.Position.Position.SecurityId,
                Value = i.Position.Position.Value,
                MarketValueInPortfolioCcy = i.Position.Position.MarketValueInPortfolioCcy,
                MarketValueInSecurityCcy = i.Position.Position.MarketValueInSecurityCcy,
                ValuationPrice = i.Position.Position.ValuationPrice,
                Weight = i.Position.Position.Weight,
        })
        .ComputeWeight(TaskName)
        .EfCoreSave($"{TaskName} Save Positions",  o => o.SeekOn(i => new {i.PortfolioCompositionId,i.SecurityId}).DoNotUpdateIfExists());
        
ProcessContextStream.WaitWhenDone("wait till everything is done", newPositionsSaved, estimatedShareClassHvsStream, estimatedPortfolioTnasStream);

List<SecurityHistoricalValue> GetEstimatedNavs(MonitoringResult result)
{
        var resultRows = GetResultItems(result,"","Node0"); 

        var estimatedNavRows = resultRows.Where(i => i.ParentCode.ToLower().Contains("estimatednav-")
                                        && i.GrandParentCode.ToLower().Contains("navestimation"))
                        .GroupBy(i => int.Parse(i.ParentCode.Split("-").Last()))
                        .Select(i=> new {ShareClassId = i.Key, Data = i.ToDictionary(j => j.Result.Code , j=>j)} );

        var estimatedNavs = estimatedNavRows.Select(i => new SecurityHistoricalValue{
                SecurityId = i.ShareClassId,
                Type = HistoricalValueType.MKT,
                Date = i.Data["AsOfDate"].Result.Date.Value,
                Value = i.Data["Value"].Result.Number.Value,
        });

        var estimatedTnaRows = resultRows.Where(i => i.ParentCode.ToLower().Contains("estimatedaum-") 
                                        && i.GrandParentCode.ToLower().Contains("navestimation"))
                        .GroupBy(i => int.Parse(i.ParentCode.Split("-").Last()))
                        .Select(i=> new {ShareClassId = i.Key, Data = i.ToDictionary(j => j.Result.Code , j=>j)} );
        
        var estimatedTnas = estimatedTnaRows.Select(i => new SecurityHistoricalValue{
                SecurityId = i.ShareClassId,
                Type = HistoricalValueType.TNA,
                Date = i.Data["AsOfDate"].Result.Date.Value,
                Value = i.Data["Value"].Result.Number.Value,
        });
        return estimatedNavs.Union(estimatedTnas).ToList();
}

List<PortfolioHistoricalValue> GetEstimatedTnas(MonitoringResult result)
{
        var resultRows = GetResultItems(result,"","Node0"); 

        var estimatedTnas = resultRows.Where(i => i.ParentCode.ToLower().Contains("estimated aum-") 
                                && i.GrandParentCode == "SubFundTNA")
                        .GroupBy(i => int.Parse(i.ParentCode.Split("-").Last()))
                        .Select(i=> new {PortfolioId = i.Key, Data = i.ToDictionary(j => j.Result.Code , j=>j)} );

        return estimatedTnas.Select(i => new PortfolioHistoricalValue{
                PortfolioId = i.PortfolioId,
                Type = HistoricalValueType.TNA,
                Date = i.Data["AsOfDate"].Result.Date.Value,
                Value = i.Data["totalTna"].Result.Number.Value,
        }).ToList();
}

Composition GetEstimatedComposition(MonitoringResult result)
{
        //PrintResultItems(result);
        var results = GetResultItems(result,"","Node0");
        string portfolioCode = results.First(i => i.GrandParentCode == "Node0" && i.ParentCode == "NavEstimation").Result.Code.Split(" - ").Last();

        DateTime compoDate = results.First(i => i.ParentCode.ToLower().Contains("estimatednav") && i.Result.Code == "AsOfDate").Result.Date.Value;

        var newPosDic = results.Where(i => i.GrandParentCode == "PositionValuation")
                        .GroupBy(i => int.Parse(i.ParentCode))
                        .Select(i=> new {SecurityId = i.Key, Data = i.ToDictionary(j=>j.Result.Code , j=>j)} );
        
        var newPositions = newPosDic.Select(i=> new Position()
        {
                SecurityId = i.SecurityId,
                Value = (i.Data["Q"].Result.Number.HasValue)? i.Data["Q"].Result.Number.Value
                        : throw new Exception($"Quantity is null: {i.Data["Q"].GrandParentCode} - {i.Data["Q"].ParentCode}-{i.Data["Q"].Result.Code}"),
                MarketValueInPortfolioCcy = (i.Data["EstimatedValue"].Result.Number.HasValue)? i.Data["EstimatedValue"].Result.Number.Value
                         : throw new Exception($"EstimatedValue is null: {i.Data["EstimatedValue"].GrandParentCode} - {i.Data["EstimatedValue"].ParentCode}-{i.Data["EstimatedValue"].Result.Code}"),
                MarketValueInSecurityCcy = i.Data["EstimatedValue"].Result.Number.Value * i.Data["FxRate"].Result.Number.Value,
                ValuationPrice =  (i.Data["Price"].Result.Number.HasValue)? i.Data["Price"].Result.Number.Value: (double?) null,
        }).ToList();
        
        newPositions = newPositions.Where(i => i.Value != 0.0).ToList();
        double total = newPositions.Sum(i => i.MarketValueInPortfolioCcy);
        foreach (var newPos in newPositions)        
            newPos.Weight = newPos.MarketValueInPortfolioCcy / total;

        return new Composition{PortfolioCode = portfolioCode, Date = compoDate, Positions = newPositions
                .Select(i=> new PositionExtended{PortfolioCode = portfolioCode, Date = compoDate, Position = i}).ToList() };
}
List<MonitoringResult> PrintResultItems(MonitoringResult result)
{
        var results = GetResultItems(result,"","Node0");
        string strHeaders = "GrandParentCode,ParentCode,Code,Label,Date,Text,Integer,Number,Percentage<br>";
        string strBody = string.Join("<br>",results
                .Select(i=>$"{i.GrandParentCode},{i.ParentCode},{i.Result.Code},{i.Result.Label},{(i.Result.Date.HasValue? i.Result.Date.Value.ToString("yyyy-MM-dd"):"")},{i.Result.Text},{(i.Result.Integer.HasValue? i.Result.Integer.Value:"")},{(i.Result.Number.HasValue? i.Result.Number.Value:"")},{(i.Result.Percentage.HasValue? i.Result.Percentage.Value:"")}"));
        throw new Exception(strHeaders+strBody);
}
class Composition
{
        public string PortfolioCode {get;set;}
        public DateTime Date {get; set;}
        public List<PositionExtended> Positions {get; set;}
}

class PositionExtended
{
        public string PortfolioCode {get; set;}
        public DateTime Date {get; set;}
        public Position Position {get; set;}
}

List<MonitoringResultExtended> GetResultItems(MonitoringResult result, string grandParentCode, string parentCode)
{
        if (result == null)
                throw new Exception("Result is null");
        List<MonitoringResultExtended> results = new List<MonitoringResultExtended>();
        
        results.Add( new MonitoringResultExtended {GrandParentCode = grandParentCode, ParentCode = parentCode, Result = result }  );

        if (result.Elements != null && result.Elements.Any())
                results.AddRange(result.Elements.SelectMany(i => GetResultItems(i, parentCode,result.Code)).ToList());
        if (result.Rows != null && result.Rows.Any())
                results.AddRange(result.Rows.SelectMany(i => GetResultItems(i, parentCode,result.Code)).ToList());
        if (result.Values != null && result.Values.Any())
                results.AddRange(result.Values.SelectMany(i => GetResultItems(i, parentCode,result.Code)).ToList());
        return results;       
}


class MonitoringResultExtended
{
        public string GrandParentCode {get; set;}
        public string ParentCode {get; set;}
        public MonitoringResult Result {get; set;}
}