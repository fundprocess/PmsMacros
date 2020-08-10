using Db=FundProcess.Pms.DataAccess.Schemas;
using DbEnums=FundProcess.Pms.DataAccess.Enums;

Regex _instrumentCodeRegex = new Regex(@"^((?<ExchangeRate>((?<from>.{3})(?<to>.{3})\s+Curncy.*?))|(?<Index>((?<IndexCode>.+)\s+index.*?)))$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
(string Index, string From, string To) ParseInstrumentCode(string instrumentCode)
{
    var match = _instrumentCodeRegex.Match(instrumentCode);
    if (!match.Success) return default;
    var index = match.Groups["IndexCode"]?.Value;
    var from = match.Groups["from"].Value;
    var to = match.Groups["to"].Value;
    return (index, from, to);
}
var rowStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse index file", FlatFileDefinition.Create(i => new
    {
        Date = i.ToDateColumn("Date", "yyyy-MM-dd"),
        InstrumentCode = i.ToColumn<string>("InstrumentCode"),
        HistoricalPrice = i.ToNumberColumn<double>("HistoricalPrice", ".")
    }).IsColumnSeparated(','))
    .Select($"{TaskName}: parse instrument code", i =>
    {
        var split = ParseInstrumentCode(i.InstrumentCode);
        if (split == default)
        {
            return default;
        }
        return new
        {
            i.Date,
            i.HistoricalPrice,
            split.Index,
            split.From,
            split.To
        };
    })
    .Where($"{TaskName}: Exclude invalid instrument code lines", i => i != null)
    .SetForCorrelation($"{TaskName}: Set correlation key");
#region indexes
var indexRowStream = rowStream
    .Where($"{TaskName}: keep only indexes", i => !string.IsNullOrWhiteSpace(i.Index));
var indexStream = indexRowStream
    .Distinct($"{TaskName}: Exclude doubles for a code", i => i.Index.ToLower(), true)
    .Select($"{TaskName}: create index instance", ProcessContextStream, (row, dbCtx) => new Db.Pms.Index
    {
        TenantId = dbCtx.TenantId,
        PricingFrequency = DbEnums.FrequencyType.Daily,
        Code = row.Index,
        Name = row.Index
    })
    .EfCoreSave($"{TaskName}: save index", o => o.SeekOn(i => i.Code));
var savedIndexHistoricalValueStream = indexRowStream
    .Distinct($"{TaskName}: Exclude doubles for a code and a date", i => new { Index = i.Index.ToLower(), i.Date }, true)
    .CorrelateToSingle($"{TaskName}: Lookup related index", indexStream, (l, r) => new
    {
        FromFile = l,
        Index = r
    })
    .Select($"{TaskName}: Create index value instance", row => new Db.Pms.IndexHistoricalValue
    {
        Date = row.FromFile.Date,
        IndexId = row.Index.Id,
        Type = DbEnums.HistoricalValueType.MKT,
        Value = row.FromFile.HistoricalPrice
    })
    .EfCoreSave($"{TaskName}: save index value", o => o.SeekOn(i => new { i.IndexId, i.Date, i.Type }));
#endregion

#region fx
var savedFxRateStream = rowStream
    .Where($"{TaskName}: keep only fx from euro as it is the reference", i => string.Equals(i.From, "eur", System.StringComparison.InvariantCultureIgnoreCase))
    .Distinct($"{TaskName}: Exclude doubles for a source, a destination and a date", i => new { From = i.From.ToLower(), To = i.To.ToLower(), i.Date }, true)
    .LookupCurrency($"{TaskName}: get related currency", l => l.To, (l, r) => new { CurrencyId = r.Id, l.Date, l.HistoricalPrice })
    .Select($"{TaskName}: Create fx rate", row => new Db.Pms.FxRate
    {
        CurrencyToId = row.CurrencyId,
        Date = row.Date,
        RateFromReferenceCurrency = row.HistoricalPrice
    })
    .EfCoreSave($"{TaskName}: Save fx rate", o => o.SeekOn(i => new { i.Date, i.CurrencyToId }));
#endregion
FileStream.WaitWhenDone($"{TaskName}: wait till everything is processed", savedIndexHistoricalValueStream, savedFxRateStream)
