var fileDefinition = FlatFileDefinition.Create(i => new
{
    SecurityName = i.ToColumn("SecurityName"),
    SecurityType = i.ToColumn("SecurityType"),
    SecurityCode = i.ToColumn("SecurityCode"),
    Isin = i.ToColumn("ISIN"),
    CcyIso = i.ToColumn("CcyIso"),
    BbgTicker = i.ToColumn("BbgTicker"),
    EodCode = i.ToColumn("EodCode"),
    CountryIso2 = i.ToColumn("CountryIso2"),
}).IsColumnSeparated(',');//.WithEncoding(System.Text.Encoding.GetEncoding(1252));

var fileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse persons file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

//Create securities
var securitiesStream = fileStream
    .Distinct($"{TaskName}: distinct positions security", i => GetInternalCode(i.Isin,i.SecurityCode))
    .LookupCurrency($"{TaskName}: get related currency", l => l.CcyIso,
        (l, r) => new { FileRow = l, Currency = r })
    .LookupCountry($"{TaskName}: get related country", l => l.FileRow.CountryIso2,
        (l, r) => new { FileRow = l.FileRow , Currency = l.Currency, Country = r })
    .Select($"{TaskName}: create instrument", i => 
        CreateSecurity(i.FileRow.SecurityCode, i.FileRow.SecurityName, null, i.FileRow.SecurityType, i.FileRow.Isin, i.Currency,i.Country)
    )
    .EfCoreSave($"{TaskName}: save Security", o => o.SeekOn(i => GetInternalCode(i.Isin,i.InternalCode)).DoNotUpdateIfExists());

//EOD Code
var saveEodCode = fileStream
    .Where($"{TaskName} Filter empty EOD code", i=> !string.IsNullOrEmpty(i.EodCode))
    .Distinct($"{TaskName}: distinct positions security EOD Code", i => i.EodCode)
    .CorrelateToSingle($"{TaskName}: get related security EOD code", securitiesStream, 
        (l, r) => new { FileRow = l, Security = r })
    .Select($"{TaskName}: create EOD code", i => new SecurityDataProviderCode
    {
        SecurityId = (i.Security != null)? i.Security.Id : throw new Exception("Security not found: "+ i.FileRow.Isin),
        Code = i.FileRow.EodCode,
        DataProvider = "EOD",
    })
    .EfCoreSave($"{TaskName}: save EOD Code", o => o.SeekOn(i => new {i.SecurityId, i.DataProvider }));

//Bloomberg Code
var saveBbgCode = fileStream
    .Where($"{TaskName} Filter empty bbg code", i=> !string.IsNullOrEmpty(i.BbgTicker))
    .Distinct($"{TaskName}: distinct positions security BBG Code", i => i.BbgTicker)
    .CorrelateToSingle($"{TaskName}: get related security bbg code", securitiesStream, 
        (l, r) => new { FileRow = l, Security = r })
    .Select($"{TaskName}: create Bloomberg code", i => new SecurityDataProviderCode
    {
        SecurityId = (i.Security != null)? i.Security.Id : throw new Exception("Security not found: "+ i.FileRow.Isin),
        Code = i.FileRow.BbgTicker,
        DataProvider = "Bloomberg",
    })
    .EfCoreSave($"{TaskName}: save Bbg Code", o => o.SeekOn(i => new {i.SecurityId, i.DataProvider }));

string GetInternalCode(string isin,string securityCode)
    => !string.IsNullOrEmpty(isin)? isin : securityCode;

SecurityInstrument CreateSecurity(string securityCode, string secName, Company issuer, 
                        string secType, string isin, Currency currency, Country country)
{
    SecurityInstrument security = null;
    switch (secType)
    {
        case "Equities":
            security = new Equity();
            break;
        case "SubFund":
        case "ShareClass":
            security = new ShareClass();
            break;
        default:
            throw new Exception("Not implemented: " + secType);
    }
    security.InternalCode = GetInternalCode(isin,securityCode);
    security.CurrencyId = (currency != null)? currency.Id : (int?) null;
    security.Name = secName;
    security.ShortName = security.Name.Truncate(MaxLengths.ShortName);

    if (security is SecurityInstrument securityInstrument)
        securityInstrument.Isin = isin;

    // if (security is Derivative der)
    //     der.MaturityDate = maturityDate;
    // if (security is StandardDerivative standardDerivative)
    //     standardDerivative.Nominal = nominal;
    // if (security is OptionFuture optFut)
    //     optFut.UnderlyingIsin = instrumentIsin;
    // if (security is Bond bond)
    // {
    //     bond.CouponFrequency = MapPeriodicity(couponPeriodicity);
    // }

    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.PricingFrequency = FrequencyType.Daily;
        regularSecurity.CountryId = (country != null)? country.Id : (int?) null;
        regularSecurity.IssuerId = (issuer != null)? issuer.Id : throw new Exception("Issuer not found for: " + secName); 
    }
    return security;
}

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", securitiesStream) ;