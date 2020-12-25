var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    Subject = i.ToColumn("Login"),
    Title = i.ToColumn("Title"),
    FirstName = i.ToColumn("FirstName"),
    LastName = i.ToColumn("LastName"),
    Email = i.ToColumn("Email"),
    MobileNumber = i.ToColumn("MobileNumber"),
    CountryIso2 = i.ToColumn("CountryIso2"),
    CcyIso = i.ToColumn("CcyIso"),
    PhoneNumber = i.ToColumn("PhoneNumber"),
    Culture = i.ToColumn("Culture"),
    IdCardNumber = i.ToColumn("IdCardNumber"),
    PassportNumber = i.ToColumn("PassportNumber"),
    StreetAddress = i.ToColumn("StreetAddress"),
    ZipCode = i.ToColumn("ZipCode"),
    Location = i.ToColumn("Location"),
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var personFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse persons file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var stream = personFileStream
    .LookupCountry($"{TaskName}: get related country for person", l => l.CountryIso2, (l, r) => new {fileRow = l, CountryId = r?.Id})
    .LookupCurrency($"{TaskName}: get related currency for person", l => l.fileRow.CcyIso, (l, r) => 
                new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = r?.Id})
    .Select($"{TaskName}: create person entity", i => new Person
    {
        InternalCode = !string.IsNullOrEmpty(i.fileRow.InternalCode)? i.fileRow.InternalCode
                        : throw new Exception("Empty line error, please check you csv content"),
        Title = i.fileRow.Title,
        FirstName = i.fileRow.FirstName,
        LastName = i.fileRow.LastName,
        Email = i.fileRow.Email,
        Subject = i.fileRow.Subject,
        MobileNumber = i.fileRow.MobileNumber,
        CurrencyId = i.CurrencyId.HasValue? i.CurrencyId.Value:(int?)null,
        CountryId = i.CountryId.HasValue? i.CountryId.Value:(int?)null,
        Culture = !string.IsNullOrEmpty(i.fileRow.Culture)? new CultureInfo(i.fileRow.Culture): throw new Exception("Empty line error (culture field), please check you csv content"),
        PhoneNumber = i.fileRow.PhoneNumber,
        IdCardNumber = i.fileRow.IdCardNumber,
        PassportNumber = i.fileRow.PassportNumber,
        StreetAddress = i.fileRow.StreetAddress,
        ZipCode = i.fileRow.ZipCode,
        Location = i.fileRow.Location,
    })
    .EfCoreSave($"{TaskName}: save person", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());
    
return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", stream);