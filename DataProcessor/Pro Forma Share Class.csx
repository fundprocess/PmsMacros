
var primaryShareClassStream = ProcessContextStream.EfCoreSelect($"{TaskName} Get primary share-class", (ctx,j) => 
    ctx.Set<ShareClass>().Where(i=> i.InternalCode == "LU2079399270"));

var proFormaShareClassStream = primaryShareClassStream.Select($"{TaskName}", (psc,j) => new ShareClass{
    InternalCode = "UI I - ValuFocus - Strategy",
    Name = "UI I - ValuFocus - Strategy",
    Isin = "LU2079399270",
    ShortName = "Strategy",
    SubFundId = psc.SubFundId,
    CurrencyId = psc.CurrencyId,

})
.EfCoreSave($"{TaskName}: Save ShareClass", o=>o.SeekOn(i => new {i.InternalCode}).DoNotUpdateIfExists()); 

var navsStream = primaryShareClassStream.EfCoreSelect($"{TaskName} Get primary share-class prices", (ctx,j) => 
    ctx.Set<SecurityHistoricalValue>().Include(i => i.Security).Where(hv => hv.SecurityId == j.Id && hv.Type == HistoricalValueType.MKT));

var saveCopyPasteNavsStream = navsStream
        .Lookup($"{TaskName}: get related pro forma share class", proFormaShareClassStream, 
            i => (i.Security as ShareClass).Isin, i => i.Isin,
            (l,r) => new { NewShareClass = r, Nav = l} )
        .Select($"{TaskName}: Copy/Paste Prices", i => new SecurityHistoricalValue{
            SecurityId = i.NewShareClass.Id,
            Type = HistoricalValueType.MKT,
            Date = i.Nav.Date,
            Value = i.Nav.Value,
        })
        .EfCoreSave($"{TaskName}: Save Copy/Paste NAV", o=>o.SeekOn(i => new {i.SecurityId,i.Type,i.Date})); 

ProcessContextStream.WaitWhenDone("wait till everything is done",saveCopyPasteNavsStream);