var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    Isin = i.ToColumn("Isin"),
    Name = i.ToColumn("Name"),
    CcyIso = i.ToColumn("CcyIso"),
}).IsColumnSeparated(',');

var secDataFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var existingSecuritiesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get sec data stream from db", 
        (ctx, j) => ctx.Set<SecurityInstrument>());

var updateSecuritiesStream = secDataFileStream
    .Lookup($"{TaskName}: Get existing Security",existingSecuritiesStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {
            FileRow = l, 
            SecurityDb = (r != null)? r: throw new Exception($"{l.InternalCode} not found in database") })
    .LookupCurrency($"{TaskName}: Get related currency", i => i.FileRow.CcyIso,
        (l,r) => new {l.FileRow, l.SecurityDb, Currency = r})
    .Select($"{TaskName}: Update security data", i => {
        i.SecurityDb.CurrencyId = i.Currency.Id;
        return i.SecurityDb;
    })
    .EfCoreSave($"{TaskName}: save Security");

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", updateSecuritiesStream);