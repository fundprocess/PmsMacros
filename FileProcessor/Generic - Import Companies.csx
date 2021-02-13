var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    Name = i.ToColumn("Name"),
    CountryIso2 = i.ToColumn("CountryIso2"),
    CcyIso = i.ToColumn("CcyIso"),
    MainContactPersonId = i.ToColumn("MainContactPersonId"),
    Culture = i.ToColumn("Culture"),
    Regulated = i.ToBooleanColumn("Regulated", "TRUE", "FALSE"),
    CssfEquivalentSupervision = i.ToBooleanColumn("CssfEquivalentSupervision", "TRUE", "FALSE"),
    DiscretionaryManagementLicensed = i.ToBooleanColumn("DiscretionaryManagementLicensed", "TRUE", "FALSE"),
    Aifm = i.ToBooleanColumn("Aifm", "TRUE", "FALSE"),
    PhoneNumber = i.ToColumn("PhoneNumber"),
    VatNumber = i.ToColumn("VatNumber"),
    Url = i.ToColumn("Url"),
    RegistrationNumber = i.ToColumn("RegistrationNumber"),
    Email = i.ToColumn("Email"),
    StreetAddress = i.ToColumn("StreetAddress"),
    ZipCode = i.ToColumn("ZipCode"),
    Location = i.ToColumn("Location"),
    YearEnd = new DateOfYear(12,31)
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var companyFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse company file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var personsStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get persons stream from db", (ctx, j) => ctx.Set<Person>());
    
var companyStream = companyFileStream
    .LookupCountry($"{TaskName}: get related country", l => l.CountryIso2, (l, r) => new {fileRow = l, CountryId = r?.Id})
    .LookupCurrency($"{TaskName}: get related currency", l => l.fileRow.CcyIso, (l, r) => 
                new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = r?.Id})
    .Lookup($"{TaskName}: get Related Main Contact Person for Company", personsStream,
        i=>i.fileRow.MainContactPersonId , i=>i.InternalCode, 
        (l,r) => new { fileRow = l.fileRow, CountryId=l.CountryId, CurrencyId = l.CurrencyId, MainContact = r})
    .Select($"{TaskName}: create company entity", i => new Company
    {
        InternalCode = !string.IsNullOrEmpty(i.fileRow.InternalCode)? i.fileRow.InternalCode
                        : throw new Exception("Empty line error, please check you csv content"),
        Name = i.fileRow.Name,
        CurrencyId = i.CurrencyId.HasValue? i.CurrencyId.Value: (int?)null,
        CountryId = i.CountryId.HasValue? i.CountryId.Value: (int?)null,
        MainContactId = i.MainContact !=null? i.MainContact.Id : (int?)null,
        Culture = !string.IsNullOrEmpty(i.fileRow.Culture)? new CultureInfo(i.fileRow.Culture): throw new Exception("Empty line error (culture field), please check you csv content"),
        Regulated = i.fileRow.Regulated,
        CssfEquivalentSupervision = i.fileRow.CssfEquivalentSupervision,
        DiscretionaryManagementLicensed = i.fileRow.DiscretionaryManagementLicensed,
        Aifm = i.fileRow.Aifm,
        PhoneNumber = i.fileRow.PhoneNumber,
        VatNumber = i.fileRow.VatNumber,
        Url = i.fileRow.Url,
        RegistrationNumber = i.fileRow.RegistrationNumber,
        Email = i.fileRow.Email,
        StreetAddress = i.fileRow.StreetAddress,
        ZipCode = i.fileRow.ZipCode,
        Location = i.fileRow.Location,
    })
    .EfCoreSave($"{TaskName}: save company", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());
