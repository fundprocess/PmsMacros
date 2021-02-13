var dbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get dbStream", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.SecurityTransaction>()
            .OrderBy(i=>i.PortfolioId).ThenBy(i=>i.TradeDate)
            .Include(i=>i.Security).Include(i=>i.Portfolio));

var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    TransactionCode = i.ToColumn<string>("TransactionCode"),
    PortfolioCode = i.ToColumn<string>("Portfolio"), 
    InternalCode = i.ToColumn<string>("SecurityCode"),
    SecurityName = i.ToColumn<string>("SecurityName"),
    TradeDate = i.ToDateColumn("TradeDate", "yyyy-MM-dd"),
    CommentEN = i.ToColumn<string>("CommentEN"),
    CommentFR = i.ToColumn<string>("CommentFR"),
    CommentNL = i.ToColumn<string>("CommentNL"),
}).IsColumnSeparated(',');

var export = dbStream
    .Select("create extract items", i => new {
        TransactionCode = i.TransactionCode,
        PortfolioCode = i.Portfolio.InternalCode,
        InternalCode = i.Security.InternalCode,
        SecurityName = i.Security.Name,
        TradeDate = i.TradeDate,
        CommentEN = (i.Comment != null)? (i.Comment.ContainsKey("en")? i.Comment["en"]:""):"",
        CommentFR = (i.Comment != null)? (i.Comment.ContainsKey("fr")? i.Comment["fr"]:""):"",
        CommentNL = (i.Comment != null)? (i.Comment.ContainsKey("nl")? i.Comment["nl"]:""):"",
    })
    .ToTextFileValue("Export to csv", $"TransactionComments - {DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss")}.csv", csvFileDefinition);
return export;