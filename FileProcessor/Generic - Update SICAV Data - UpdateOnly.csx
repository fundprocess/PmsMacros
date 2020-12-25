var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    Name = i.ToColumn("Name"),
    SicavType = i.ToColumn("SicavType"),
    LegalForm = i.ToColumn("LegalForm"),
    RefCcy = i.ToColumn("RefCcy"),
    Country = i.ToColumn("Country"),
    ManCo = i.ToColumn("ManCo"),
}).IsColumnSeparated(',');

var sicavsFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse sub fund data file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var sicavsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get sub fund stream from db", (ctx, j) => ctx.Set<Sicav>());

var savedSicavstream = sicavsFileStream
    .Lookup($"{TaskName}: Get existing sub fund",sicavsDbStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {FileRow = l, SicavDb = r})
    .Where($"{TaskName}: Keep not null", i => i.SicavDb != null)
    .LookupCountry($"{TaskName}: Get Related Country", i => i.FileRow.Country,
        (l,r) => new {FileRow = l.FileRow, SicavDb = l.SicavDb, Country = r})
    .LookupCurrency($"{TaskName}: Get Related Currency", i => i.FileRow.RefCcy,
        (l,r) => new {FileRow = l.FileRow, SicavDb = l.SicavDb, Country = l.Country, Currency = r})
    .Select($"{TaskName}: Update sicav", i => {       
        i.SicavDb.Name = !string.IsNullOrEmpty(i.FileRow.Name)? i.FileRow.Name : 
            (i.SicavDb != null? i.SicavDb.Name: throw new Exception("Sicav not found: "+ i.FileRow.InternalCode) );
        i.SicavDb.CountryId = i.Country != null ? i.Country.Id : (int?) null;
        i.SicavDb.CurrencyId = i.Currency != null ? i.Currency.Id : (int?) null;
        i.SicavDb.LegalForm = (LegalForm) Enum.Parse(typeof(LegalForm), i.FileRow.LegalForm, true);
        i.SicavDb.SicavType = (SicavType)Enum.Parse(typeof(SicavType), i.FileRow.SicavType, true);
        return i.SicavDb;
    })
    .EfCoreSave($"{TaskName}: save sicav");

#region roles
var manCosDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get manCos from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i=>i.Role).Include(i=>i.Entity)
                        .Where(i=> i.Role.Code == "ManagementCompany"));
var manCosStream = sicavsFileStream
    .Distinct($"{TaskName}: Distinct sicav internal code for manco", i=>i.InternalCode)
    .Lookup($"{TaskName}: lookup manco", manCosDbStream, i => i.ManCo, i => i.Entity.InternalCode,
        (l,r) => new {FileRow = l, Manco = r})
    .Lookup($"{TaskName} get related sicavDb", sicavsDbStream, i => i.FileRow.InternalCode, i => i.InternalCode,
        (l,r) => new {l.FileRow, l.Manco, SicavDb = r})
    .Where($"{TaskName}: Keep sicav not null", i => i.SicavDb != null)
    .Select($"{TaskName}: create link Manco", i => new RelationshipSicav()
    {
        SicavId = (i.SicavDb != null)? i.SicavDb.Id : throw new Exception("Sicav not found: " + i.FileRow.InternalCode),
        RelationshipId = (i.Manco != null)? i.Manco.Id : throw new Exception("ManCo not found: " + i.FileRow.ManCo) ,
    })    
    .EfCoreSave($"{TaskName}: save link Manco", o => o
        .SeekOn(i => new {i.SicavId , i.RelationshipId}).DoNotUpdateIfExists());
#endregion


return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", 
    savedSicavstream,
    manCosStream
    );