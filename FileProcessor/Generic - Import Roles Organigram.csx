var fileDefinition = FlatFileDefinition.Create(i => new
{
    RoleCode = i.ToColumn("RoleCode"),
    EntityCode = i.ToColumn("EntityCode"),
    Currency  = i.ToColumn("Currency"),
    IsMainInRole = i.ToBooleanColumn("IsMainInRole","TRUE","FALSE"),
    StartDate = i.ToDateColumn("StartDate", "yyyy-MM-dd"),
    EndDate = i.ToOptionalDateColumn("EndDate", "yyyy-MM-dd"),
}).IsColumnSeparated(',');

var manCoRolesFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse roles file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var roleDefinitionsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get role definitions stream from db", (ctx, j) => ctx.Set<Role>());
var entityStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get entities stream from db", (ctx, j) => ctx.Set<Entity>());
    
var roleRelationshipsStream = manCoRolesFileStream
    .Lookup($"{TaskName}: get Related Role Def", roleDefinitionsStream,
        i=>i.RoleCode, i=>i.Code, (l,r) => new { FileRow = l, RoleDefinition = r})
    .Lookup($"{TaskName}: get Related Entity", entityStream,
        i=>i.FileRow.EntityCode, i=>i.InternalCode, (l,r) => new { FileRow = l.FileRow, RoleDefinition = l.RoleDefinition, Entity = r})
    .LookupCurrency($"{TaskName}: get Related Currency", i=>i.FileRow.Currency, 
        (l,r) => new { FileRow = l.FileRow, RoleDefinition = l.RoleDefinition, Entity = l.Entity, Currency = r})
    .Select($"{TaskName}: create Role Relationship ", i => new RoleRelationship
    {
        RoleId = i.RoleDefinition !=null ? i.RoleDefinition.Id : throw new Exception($"Role Definition not found for role code {i.FileRow.RoleCode}"),
        EntityId = i.Entity != null? i.Entity.Id : throw new Exception($"Entity not found with internal code {i.FileRow.EntityCode}"),
        CurrencyId = i.Currency!=null? i.Currency.Id : throw new Exception($"Currency not found with internal code {i.FileRow.EntityCode}"),
        StartDate= i.FileRow.StartDate,
        EndDate=i.FileRow.EndDate,
    })
    .EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => new {i.RoleId , i.EntityId}).DoNotUpdateIfExists());

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", roleRelationshipsStream);