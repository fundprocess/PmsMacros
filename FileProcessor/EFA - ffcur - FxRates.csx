var efaCurrencyFileDefinition = FlatFileDefinition.Create(i => new
{
    CotationDate = i.ToDateColumn("Cotation_date", "dd/MM/yyyy"),
    CurrencyCode = i.ToColumn("Currency_code"),
    FixingCurrency = i.ToColumn("Fixing_currency"),
    ExchangeRate = i.ToNumberColumn<double>("Exchange_rate", "."),
    Quotity = i.ToNumberColumn<double>("Quotity", "."),
}).IsColumnSeparated(',');

var currencyFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse nav file", efaCurrencyFileDefinition);
var currencyFileSortedByDateAndFixingStream = currencyFileStream
    .Sort($"{TaskName}: sort by date and fixing", i => new { i.CotationDate });
var euroCurrencyFileSortedByDateStream = currencyFileStream
    .Where($"{TaskName}: get euro target only", i => string.Equals(i.CurrencyCode, "EUR", System.StringComparison.InvariantCultureIgnoreCase))
    .Sort($"{TaskName}: sort by date", i => new { i.CotationDate })
    .Distinct($"{TaskName}: distinct target on euro per date");

var savedFxRates = currencyFileSortedByDateAndFixingStream
    .LeftJoin($"{TaskName}: get fixing to euro", euroCurrencyFileSortedByDateStream, (l, r) => new
    {
        To = l.CurrencyCode,
        Date = l.CotationDate,
        Rate = (l.ExchangeRate / l.Quotity) / (r.ExchangeRate / r.Quotity)
    })
    .Distinct($"{TaskName}: Exclude doubles for a destination and a date", i => new { To = i.To.ToLower(), i.Date }, true)
    .LookupCurrency($"{TaskName}: get related currency", l => l.To, (l, r) => new { CurrencyId = r.Id, l.Date, l.Rate })
    .Select($"{TaskName}: Create fx rate", row => new FxRate
    {
        CurrencyToId = row.CurrencyId,
        Date = row.Date,
        RateFromReferenceCurrency = row.Rate
    })
    .EfCoreSave($"{TaskName}: Save fx rate", o => o.SeekOn(i => new { i.Date, i.CurrencyToId }));
return FileStream.WaitWhenDone($"{TaskName}: wait end of all save", savedFxRates);
