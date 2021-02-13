var fileDefinition = FlatFileDefinition.Create(i => new
{
    //TransactionCode,Portfolio,SecurityCode,SecurityName,TradeDate,CommentEN,CommentFR,CommentNL
    TransactionCode = i.ToColumn("TransactionCode"),
    //SecurityDesc = i.ToColumn("Security Desc"),
    SecurityCode = i.ToColumn("SecurityCode"),
    CommentEN = i.ToColumn("CommentEN"),
    CommentFR = i.ToColumn("CommentFR"),
    CommentNL = i.ToColumn("CommentNL")
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var secTransFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse transaction file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var secTransDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get db SecTrans stream", 
        (ctx, j) => ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.SecurityTransaction>());

var updatedSecTransStream = secTransFileStream
    .Where($"{TaskName}: Where: Keep not empty", i => !string.IsNullOrWhiteSpace(i.CommentEN) || !string.IsNullOrWhiteSpace(i.CommentFR) || !string.IsNullOrWhiteSpace(i.CommentNL)  )
    .Distinct($"{TaskName}: Distinct secTransFile", i=>i.TransactionCode)
    .Lookup($"{TaskName}: Get related DB secTrans",secTransDbStream, i=>i.TransactionCode,i=>i.TransactionCode,
        (l,r) => new {FileRow = l, Existing = r})
    .Where($"{TaskName}: Keep existing", i => i.Existing!=null)
    .Select($"{TaskName}: Update comment", i => {
        i.Existing.Comment = new MultiCultureString { ["en"] = i.FileRow.CommentEN, ["fr"] = i.FileRow.CommentFR, ["nl"] = i.FileRow.CommentNL };
        return i.Existing;
    })
    .EfCoreSave($"{TaskName}: save updatedSecTransStream");

return FileStream.WaitWhenDone($"{TaskName}: wait end of all save", updatedSecTransStream);