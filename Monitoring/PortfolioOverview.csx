var parentSection = NewSection("Parent","Parent");
var section1 = parentSection.AddSection("Nav Summary","NavSummary");

var portfolios = Referential.Portfolios.Where(i => i.Id == Parameters.SubFundId);
var navSummary =  portfolios.Select(i =>
            NewListRow($"{i.Name}", new[] {
                NewValue("Ccy").SetText(i.CcyIso),
                NewValue("Date").SetDate(i.NavDate.Value),
                NewValue("NAV").SetNumber(i.Nav),
                NewValue("NAV T-1","previousNav").SetNumber(i.PreviousNav),
                NewValue("% Change","pChange").SetPercentage( (i.Nav/i.PreviousNav)-1)
                    .SetDirection( GetNavDirection((i.Nav/i.PreviousNav)-1) )
                    .SetSentiment( ((i.Nav/i.PreviousNav)-1) > 0 ? MacroResultSentiment.Positive : MacroResultSentiment.Negative)
        }))
        .ToList();

var tnaSummary =  Referential.Portfolios
        .Where(i => i.Id == Parameters.SubFundId)
        .Select(i =>
            NewListRow($"{i.Name}", new[] {
                NewValue("Ccy").SetText(i.CcyIso),
                NewValue("Date").SetDate(i.NavDate.Value),
                NewValue("TNA").SetNumber(i.Tna),
                NewValue("TNA T-1","previousNav").SetNumber(i.PreviousTna),
                NewValue("% Change","pChange").SetPercentage( (i.Tna/i.PreviousTna)-1)
                    .SetDirection( GetNavDirection((i.Tna/i.PreviousTna)-1) )
                    .SetSentiment( ((i.Tna/i.PreviousTna)-1) > 0 ? MacroResultSentiment.Positive : MacroResultSentiment.Negative)
        }))
        .ToList();

section1.AddList("NAV Summary","NavSummary", navSummary);
section1.AddList("TNA Summary","TnaSummary", tnaSummary);

var section2 = parentSection.AddSection("Performances vs Volatilities (Annualized)","PerformancesVsVolatilitesTab");
foreach (var p in portfolios)
{
    var performances = Referential.Performances.FirstOrDefault(i => i.PortfolioId == p.Id);
    if (performances != null)
        section2.AddList($"{p.Name} Performances","PerformancesList", GetTab(p.Name,StatType.Performance ,performances));

    var volatilities = Referential.AnnualizedVolatilities.FirstOrDefault(i => i.PortfolioId == p.Id);
    if (volatilities != null)
        section2.AddList($"{p.Name} Ann. Volatilities","VolatilitiesList", GetTab(p.Name,StatType.Volatility,volatilities));
}


#region Helpers
public enum StatType
{
    Performance,
    Volatility,
}
List<MacroResultRow> GetTab(string portfolioName,StatType type, StatisticsTab values)
{
    var performancesTab = new List<MacroResultRow>();

    var portfolioPerfRow = NewListRow($"{portfolioName}");
    portfolioPerfRow.Values.Add(NewValue("Date").SetDate(values.Date));
    portfolioPerfRow.Values.Add(values.OneDay.HasValue? NewValue("1D").SetPercentage(values.OneDay.Value) : NewValue("1D").SetText("-"));
    portfolioPerfRow.Values.Add(values.FiveDays.HasValue? NewValue("1W").SetPercentage(values.FiveDays.Value)  : NewValue("1W").SetText("-"));
    portfolioPerfRow.Values.Add(values.OneMonth.HasValue? NewValue("1M").SetPercentage(values.OneMonth.Value)  : NewValue("1M").SetText("-"));
    portfolioPerfRow.Values.Add(values.ThreeMonths.HasValue? NewValue("3M").SetPercentage(values.ThreeMonths.Value)  : NewValue("3M").SetText("-"));
    portfolioPerfRow.Values.Add(values.SixMonths.HasValue? NewValue("6M").SetPercentage(values.SixMonths.Value)  : NewValue("6M").SetText("-"));
    portfolioPerfRow.Values.Add(values.YearToDate.HasValue? NewValue("YTD").SetPercentage(values.YearToDate.Value)  : NewValue("YTD").SetText("-"));
    portfolioPerfRow.Values.Add(values.OneYear.HasValue? NewValue("1Y").SetPercentage(values.OneYear.Value)  : NewValue("1Y").SetText("-"));        
    performancesTab.Add(portfolioPerfRow);

    var benchmarkPerfRow = NewListRow("Benchmark");
    benchmarkPerfRow.Values.Add(NewValue("Date").SetDate(values.Date));
    benchmarkPerfRow.Values.Add(values.OneDayBenchmark.HasValue? NewValue("1D").SetPercentage(values.OneDayBenchmark.Value): NewValue("1D").SetText("-"));
    benchmarkPerfRow.Values.Add(values.FiveDaysBenchmark.HasValue? NewValue("1W").SetPercentage(values.FiveDaysBenchmark.Value) : NewValue("1W").SetText("-"));
    benchmarkPerfRow.Values.Add(values.OneMonthBenchmark.HasValue? NewValue("1M").SetPercentage(values.OneMonthBenchmark.Value) : NewValue("1M").SetText("-"));
    benchmarkPerfRow.Values.Add(values.ThreeMonthsBenchmark.HasValue? NewValue("3M").SetPercentage(values.ThreeMonthsBenchmark.Value) : NewValue("3M").SetText("-"));
    benchmarkPerfRow.Values.Add(values.SixMonthsBenchmark.HasValue? NewValue("6M").SetPercentage(values.SixMonthsBenchmark.Value) : NewValue("6M").SetText("-"));
    benchmarkPerfRow.Values.Add(values.YearToDateBenchmark.HasValue? NewValue("YTD").SetPercentage(values.YearToDateBenchmark.Value) : NewValue("YTD").SetText("-"));
    benchmarkPerfRow.Values.Add(values.OneYearBenchmark.HasValue? NewValue("1Y").SetPercentage(values.OneYearBenchmark.Value) : NewValue("1Y").SetText("-"));
    performancesTab.Add(benchmarkPerfRow);

    var alphaRow = NewListRow(type==StatType.Performance? "--------- ALPHA ---------" : "--------- RISK REDUCTION ---------");
    alphaRow.Values.Add(NewValue("Date").SetDate(values.Date));
    alphaRow.Values.Add(values.OneDay.HasValue? NewValue("1D").SetPercentage(GetAlpha(type,values.OneDay.Value, values.OneDayBenchmark.Value)).SetSentiment(GetSentiment(type,values.OneDay.Value, values.OneDayBenchmark.Value)) : NewValue("1D").SetText("-"));
    alphaRow.Values.Add(values.FiveDays.HasValue? NewValue("1W").SetPercentage(GetAlpha(type,values.FiveDays.Value, values.FiveDaysBenchmark.Value)).SetSentiment(GetSentiment(type,values.FiveDays.Value, values.FiveDaysBenchmark.Value)) : NewValue("1W").SetText("-"));
    alphaRow.Values.Add(values.OneMonth.HasValue? NewValue("1M").SetPercentage(GetAlpha(type,values.OneMonth.Value, values.OneMonthBenchmark.Value)).SetSentiment(GetSentiment(type,values.OneMonth.Value, values.OneMonthBenchmark.Value)) : NewValue("1M").SetText("-"));
    alphaRow.Values.Add(values.ThreeMonths.HasValue? NewValue("3M").SetPercentage(GetAlpha(type,values.ThreeMonths.Value, values.ThreeMonthsBenchmark.Value)).SetSentiment(GetSentiment(type,values.ThreeMonths.Value, values.ThreeMonthsBenchmark.Value)) : NewValue("3M").SetText("-"));
    alphaRow.Values.Add(values.SixMonths.HasValue? NewValue("6M").SetPercentage(GetAlpha(type,values.SixMonths.Value, values.SixMonthsBenchmark.Value)).SetSentiment(GetSentiment(type,values.SixMonths.Value, values.SixMonthsBenchmark.Value)) : NewValue("6M").SetText("-"));
    alphaRow.Values.Add(values.YearToDate.HasValue? NewValue("YTD").SetPercentage(GetAlpha(type,values.YearToDate.Value, values.YearToDateBenchmark.Value)).SetSentiment(GetSentiment(type,values.YearToDate.Value, values.YearToDateBenchmark.Value)) : NewValue("YTD").SetText("-"));
    alphaRow.Values.Add(values.OneYear.HasValue? NewValue("1Y").SetPercentage(GetAlpha(type,values.OneYear.Value, values.OneYearBenchmark.Value)).SetSentiment(GetSentiment(type,values.OneYear.Value, values.OneYearBenchmark.Value)) : NewValue("1Y").SetText("-"));
    performancesTab.Add(alphaRow);    

    return performancesTab;
}
double GetAlpha(StatType type, double portValue, double benchValue)
    => type == StatType.Performance? (portValue - benchValue) : (-1 * ((portValue/benchValue)-1));

MacroResultSentiment GetSentiment(StatType type, double portValue, double benchValue)
    => type == StatType.Performance? (portValue > benchValue)? MacroResultSentiment.Positive : MacroResultSentiment.Negative
        : (portValue < benchValue)? MacroResultSentiment.Positive : MacroResultSentiment.Negative;

MacroResultDirection GetNavDirection(double? value)
{
    if (!value.HasValue)
        return MacroResultDirection.Still;
    
    if (Math.Abs(value.Value) < 0.002)
        return MacroResultDirection.Still;

    if (value.Value < 0)
        return MacroResultDirection.Down;

    return MacroResultDirection.Up;
}


#endregion Helpers

parentSection


