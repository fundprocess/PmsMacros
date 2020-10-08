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
    .Distinct($"{TaskName}: distinct on trade books", i => new { i.PortfolioCode, i.Date })
    .LookupPortfolio($"{TaskName}: lookup for portfolio and create tradebook", i =>i.PortfolioCode, 
            (l, r) => new TradeBook { PortfolioId = r.Id, Date = l.Date } )
    .EfCoreSave($"{TaskName}: save trade books", o => o.SeekOn(i => new { i.PortfolioId, i.Date }).DoNotUpdateIfExists());

//--------------------------
//Saving trades
//--------------------------
//Get all securities in a dictionary

var tradesStream = tradeFileStream
    .CorrelateToSingle($"{TaskName}: get related tradebook", tradeBookStream, (l, r) => new { FileRow = l, TradeBook = r })
    .EfCoreLookup($"{TaskName}: get related security", o=> o.LeftJoinEntity(i=>i.FileRow.SecurityCode,(Security s)=>s.InternalCode,
                            (l, r) => new { l.FileRow, l.TradeBook , Security = r }).CacheFullDataset())
    .EfCoreLookup($"{TaskName}: get related PM", o=> o.LeftJoinEntity(i=>i.FileRow.PlacedBy,(Person p) =>p.InternalCode,
                            (l,r) => new {FileRow = l.FileRow, Security= l.Security, Person = r, l.TradeBook }))
    .Select($"{TaskName}: Create trades", i => new Trade 
    { 
        TradeBookId = i.TradeBook.Id,
        SecurityId =i.Security.Id,
        BuySell  = string.Equals(i.FileRow.BuySell, "buy", StringComparison.InvariantCultureIgnoreCase) ? BuySell.Buy : BuySell.Sell,
        Quantity = i.FileRow.Quantity,
        AmountInPtfCcy = i.FileRow.AmountInPtfCcy,
        PlacedById = i.Person.Id
    });

return FileStream.WaitWhenDone($"{TaskName}: Wait till everything is saved", tradesStream);
