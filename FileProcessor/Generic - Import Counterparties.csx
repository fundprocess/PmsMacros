var fileDefinition = FlatFileDefinition.Create(i => new
{
    EntityCode = i.ToColumn("EntityCode"),
    StartDate = i.ToDateColumn("StartDate","yyyy-MM-dd"),
    EndDate = i.ToOptionalDateColumn("EndDate","yyyy-MM-dd"),
    CounterpartyType = i.ToColumn("CounterpartyType"),
    EmirClassification = i.ToColumn("EmirClassification"),
    CcyIso = i.ToColumn("CcyIso"),
    Authorized = i.ToBooleanColumn("Authorized","TRUE","FALSE"),
    LastAuthorizationChange = i.ToDateColumn("LastAuthorizationChange","yyyy-MM-dd"),
}).IsColumnSeparated(',');

var counterpartiesFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse roles file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var entityStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get entities stream from db", (ctx, j) => ctx.Set<Entity>());
    
var euroCurrency = ProcessContextStream
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "EUR"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var stream = counterpartiesFileStream
    .Lookup($"{TaskName}: get Related Entity", entityStream,
        i=>i.EntityCode, i=>i.InternalCode, (l,r) => new { FileRow = l, Entity = r})
    .LookupCurrency($"{TaskName}: get Related Currency", i => i.FileRow.CcyIso, 
        (l,r) => new { FileRow = l.FileRow, Entity = l.Entity, Currency= r})
    .Select($"{TaskName}: create Role Relationship ",euroCurrency, (i,j) => new CounterpartyRelationship
    {
        EntityId = i.Entity != null? i.Entity.Id : throw new Exception($"Entity not found with internal code {i.FileRow.EntityCode}"),
        StartDate = i.FileRow.StartDate,
        EndDate = i.FileRow.EndDate,
        CurrencyId = i.Currency != null? i.Currency.Id : j.Id,
        CounterpartyType = (CounterpartyType) Enum.Parse(typeof(CounterpartyType), i.FileRow.CounterpartyType, true),
        EmirClassification = (EmirClassification) Enum.Parse(typeof(EmirClassification), i.FileRow.EmirClassification, true),
        IsAuthorized = i.FileRow.Authorized,
        LastAuthorizationChange = i.FileRow.LastAuthorizationChange,
    })
    .EfCoreSave($"{TaskName}: save counterparty relationships", o => o.SeekOn(i => i.EntityId).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", stream);

//CounterpartyType: Broker, Bank, InvestmentManager , InsuranceCompany
//EmirClassification: Financial, NonFinancial
