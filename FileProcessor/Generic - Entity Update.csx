var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    PreviousName = i.ToColumn("PreviousName"),
    EntityType = i.ToColumn("EntityType"),
    PortfolioName = i.ToColumn("PortfolioName"),
    PortfolioShortName = i.ToColumn("PortfolioShortName"),
    NewInternalCode = i.ToColumn("NewInternalCode"),
    CompanyName = i.ToColumn("CompanyName"),
    Title = i.ToColumn("Title"),
    FirstName = i.ToColumn("FirstName"),
    LastName = i.ToColumn("LastName"),
    Tel = i.ToColumn("Tel"),
    Street = i.ToColumn("Street"),
    Zip = i.ToColumn("Zip"),
    Location = i.ToColumn("Location"),
    Country = i.ToColumn("Country"),
    Mail = i.ToColumn("Mail"),
    Url = i.ToColumn("Url")
}).IsColumnSeparated(',');

var fileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse file", fileDefinition);
    //.SetForCorrelation($"{TaskName}: Set correlation key");

var existingShareClassesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get share class stream from db", 
        (ctx, j) => ctx.Set<ShareClass>().Where(i=>i.SubFundId.HasValue));
var existingPortfolioStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get portfolio stream from db", (ctx, j) => ctx.Set<Portfolio>());
var existingPeopleStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get people stream from db", (ctx, j) => ctx.Set<Person>());
var existingCompanyStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get company stream from db", (ctx, j) => ctx.Set<Company>());

var peopleStream = fileStream
    .Where($"{TaskName}: Where: Keep entitytype == person", i => i.EntityType == "Person")
    .Lookup($"{TaskName}: Get existing people",existingPeopleStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {FileRow = l, Existing = r})
     .LookupCountry($"{TaskName}: Get related person country", i => i.FileRow.Country,
        (l,r) => new {l.FileRow, l.Existing, Country = r})
    .Where($"{TaskName}: Keep not null people", i => i.Existing!=null)
    .Select($"{TaskName}: Update person", i => {
        i.Existing.InternalCode = !string.IsNullOrEmpty(i.FileRow.NewInternalCode)? i.FileRow.NewInternalCode:i.Existing.InternalCode;
        i.Existing.Title = !string.IsNullOrEmpty(i.FileRow.Title)? i.FileRow.Title: i.Existing.Title; 
        i.Existing.FirstName = !string.IsNullOrEmpty(i.FileRow.FirstName)? i.FileRow.FirstName: i.Existing.FirstName;
        i.Existing.LastName = !string.IsNullOrEmpty(i.FileRow.LastName)? i.FileRow.LastName: i.Existing.LastName;

        i.Existing.PhoneNumber = !string.IsNullOrEmpty(i.FileRow.Tel)? i.FileRow.Tel : i.Existing.PhoneNumber;
        i.Existing.StreetAddress = !string.IsNullOrEmpty(i.FileRow.Street)? i.FileRow.Street : i.Existing.StreetAddress;
        i.Existing.ZipCode = !string.IsNullOrEmpty(i.FileRow.Zip)? i.FileRow.Zip : i.Existing.ZipCode;
        i.Existing.Location = !string.IsNullOrEmpty(i.FileRow.Location)? i.FileRow.Location : i.Existing.Location;
        i.Existing.CountryId = (i.Country != null)? i.Country.Id : i.Existing.CountryId;
        i.Existing.Email = !string.IsNullOrEmpty(i.FileRow.Mail)? i.FileRow.Mail : i.Existing.Email;

        return i.Existing;
    })
    .EfCoreSave($"{TaskName}: save updated people");

var companyStream = fileStream
    .Where($"{TaskName}: Where: Keep entitytype == Company", i => i.EntityType == "Company")
    .Lookup($"{TaskName}: Get existing companies", existingCompanyStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {FileRow = l, Existing = r})
    .LookupCountry($"{TaskName}: Get related company country", i => i.FileRow.Country,
        (l,r) => new {l.FileRow, l.Existing, Country = r})
    .Where($"{TaskName}: Keep not null companies", i => i.Existing!=null)
    .Select($"{TaskName}: Update companies", i => {
        i.Existing.InternalCode = !string.IsNullOrEmpty(i.FileRow.NewInternalCode)? i.FileRow.NewInternalCode:i.Existing.InternalCode;
        i.Existing.Name = !string.IsNullOrEmpty(i.FileRow.CompanyName)? i.FileRow.CompanyName : i.Existing.Name;
        
        i.Existing.PhoneNumber = !string.IsNullOrEmpty(i.FileRow.Tel)? i.FileRow.Tel : i.Existing.PhoneNumber;
        i.Existing.StreetAddress = !string.IsNullOrEmpty(i.FileRow.Street)? i.FileRow.Street : i.Existing.StreetAddress;
        i.Existing.ZipCode = !string.IsNullOrEmpty(i.FileRow.Zip)? i.FileRow.Zip : i.Existing.ZipCode;
        i.Existing.Location = !string.IsNullOrEmpty(i.FileRow.Location)? i.FileRow.Location : i.Existing.Location;
        i.Existing.CountryId = (i.Country != null)? i.Country.Id : i.Existing.CountryId;
        i.Existing.Email = !string.IsNullOrEmpty(i.FileRow.Mail)? i.FileRow.Mail : i.Existing.Email;
        i.Existing.Url = !string.IsNullOrEmpty(i.FileRow.Url)? i.FileRow.Url : i.Existing.Url;

        return i.Existing;

    })
    .EfCoreSave($"{TaskName}: save updated companies");

var portfolioStream = fileStream
    .Lookup($"{TaskName}: Get existing portfolio",existingPortfolioStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {FileRow = l, Existing = r})
    .Where($"{TaskName}: Keep not null portfolio", i => i.Existing!=null)
    .Select($"{TaskName}: Update portfolio", i => {
        i.Existing.Name = !string.IsNullOrEmpty(i.FileRow.PortfolioName) ? i.FileRow.PortfolioName : i.Existing.Name;
        i.Existing.ShortName = !string.IsNullOrEmpty(i.FileRow.PortfolioShortName)? i.FileRow.PortfolioShortName : i.Existing.ShortName;
        return i.Existing;
    })
    .EfCoreSave($"{TaskName}: save updated portfolio");

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", peopleStream,companyStream,portfolioStream);