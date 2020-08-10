var entityFileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn<string>("Code"),
    FirstName = i.ToColumn<string>("First Name"),
    LastName = i.ToColumn<string>("Last Name"),
    Email = i.ToColumn<string>("Mail"),
    Subject = i.ToColumn<string>("Login"),
}).IsColumnSeparated(',');

ProcessContextStream
    .EfCoreSelect("Get every entity", i=>i.Set<Person>())
    .Select("Map to data", i=> new 
    {
        i.InternalCode,
        i.FirstName,
        i.LastName,
        i.Email,
        i.Subject
    })
    .ToTextFileValue("Export to csv", "entities.csv", entityFileDefinition)