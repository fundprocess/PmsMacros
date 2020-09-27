var tradeBookFileDefinition = FlatFileDefinition.Create(i => new
{
    PortfolioCode = i.ToColumn<string>("PortfolioCode"),
    Date = i.ToDateColumn("Date", "yyyyMMdd"),
    SecurityCode = i.ToColumn<string>("SecurityCode"),
    Isin = i.ToColumn<string>("ISIN"),
    BuySell = i.ToColumn<string>("BuySell"),
    Quantity = i.ToNumberColumn<double?>("Quantity", "."),
    AmountInPtfCcy = i.ToNumberColumn<double?>("AmountInPtfCcy", "."),
    PlacedBy = i.ToColumn<string>("PlacedBy")
}).IsColumnSeparated(',');

var tradeFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: Parse tradebook file", tradeBookFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//--------------------------
//Saving trade books: one per portfolio per date
//--------------------------
var tradeBookStream = tradeFileStream
    .Distinct($"{TaskName}: distinct on trade books", i => new { i.PortfolioCode, i.Date }, true)
    .LookupPortfolio($"{TaskName}: lookup for portfolio", i => i.PortfolioCode, (l, r) => new { FileRow = l, Portfolio = r })
    .Select($"{TaskName}: create trade books", i => new TradeBook { PortfolioId = i.Portfolio?.Id ?? 0, Date = i.FileRow.Date })
    .EfCoreSave($"{TaskName}: save trade books", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

//--------------------------
//Saving trades
//--------------------------
//Get all securities in a dictionary
var getSecurityId = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get securities dictionary", i => i.Set<Security>())
    .ToDictionary(i=>i.InternalCode,i=>i.Id);

//Get all relationships in a dictionary
var getRelationshipId = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get relationships dictionary", i => i.Set<RoleRelationship>().Include(i=>i.Entity)
                        .Select(r=>new {EntityCode = r.Entity.InternalCode, RelationshipId = r.Id}))
    .GroupBy(i => i.EntityCode).Select(g => g.First())
    .ToDictionary(i=>i.EntityCode,i=>i.RelationshipId);

var tradesStream = tradeFileStream
    .CorrelateToSingle($"{TaskName}: get related tradebook", tradeBookStream, (l, r) => new { FileRow = l, Tradebook = r })
    //.LookupSecurity($"{TaskName}: get related security", i => i.SecurityCode, (l, r) => new { FileRow = l, Security = r })
    .Select($"{TaskName}: Create trades", i => new Trade 
    { 
        TradeBookId = i.TradeBook.Id,
        Isin = i.FileRow.ISIN,
        SecurityId = getSecurityId[i.FileRow.SecurityCode],
        BuySell  = string.Equals(i.FileRow.BuySell, "buy", StringComparison.InvariantCultureIgnoreCase) ? BuySell.Buy : BuySell.Sell,
        Quantity = i.FileRow.Quantity,
        AmountInPtfCcy = i.FileRow.AmountInPtfCcy,
        PlacedById = getRelationshipId[i.FileRow.PlaceBy]
    })

return FileStream.WaitWhenDone($"{TaskName}: Wait till everything is saved", tradeBookStream, tradesStream);
