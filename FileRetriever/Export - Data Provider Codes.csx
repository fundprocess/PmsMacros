var dbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get dbStream", (ctx, j) => 
        ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.SecurityInstrument>().Include (i=> i.Currency)
        .Join(
            ctx.Set<FundProcess.Pms.DataAccess.Schemas.Pms.SecurityDataProviderCode>(),
            i => i.Id,
            i => i.SecurityId,
            (l,r) => new {l.InternalCode, l.Name, l.Isin, l.Currency.IsoCode, r.DataProvider, r.Code})
            //(l,r) => new {Security = l , DataProviderCode = r.FirstOrDefault()})
        );

var csvFileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn<string>("InternalCode"),
    SecurityName = i.ToColumn<string>("Security"),
    Isin = i.ToColumn<string>("Isin"),
    Ccy = i.ToColumn<string>("Ccy"),
    DataProvider = i.ToColumn<string>("DataProvider"),
    Code = i.ToColumn<string>("Code"),
}).IsColumnSeparated(',');

var export = dbStream
    .Select("create extract items", i => new {
        InternalCode = i.InternalCode,
        SecurityName = i.Name,
        Isin = i.Isin,
        Ccy = i.IsoCode,
        DataProvider = i.DataProvider,
        Code = i.Code
    })
    .ToTextFileValue("Export to csv", $"DataProviderCode - {DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss")}.csv", csvFileDefinition);

return export;