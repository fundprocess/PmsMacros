IProxyMappingResult GetRule(Security security, ProxyMappingResultProvider resultProvider)
{
    switch (security)
    {
        case Equity:
        case ShareClass:
        case Etf:
            return resultProvider.LinearRegressionIndexMapping("3LHE", "SP500BDT", "MSCIEF", "SX5E", "SPXNTR", "FCHI", "JPXNK400", "N300", "NDX", "EUU");
        case Bond bond:
            if (string.Equals(bond.Country.Region["en"], "Europe", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("3LHE", 0.8).AddIndexMapping("SP500BDT", 0.2);
            else 
                return resultProvider.SimpleIndexMapping("SP500BDT");
        case RegularSecurity regularSecurity:
            if (string.Equals(regularSecurity.Currency.IsoCode, "JPY", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("JPN");
            else if (string.Equals(regularSecurity.Currency.IsoCode, "CNY", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("FTXIN9");
            else if (string.Equals(regularSecurity.Country.Region["en"], "Asia", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("AW07");
            else if (string.Equals(regularSecurity.Country.Region["en"], "Europe", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("SX5E");
            else if (string.Equals(regularSecurity.Currency.IsoCode, "USD", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("SPXNTR");
            else if (string.Equals(regularSecurity.Currency.IsoCode, "CAD", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("GSPTSE");
            else if (string.Equals(regularSecurity.Country.Region["en"], "Oceania", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("AORD");
            else if (string.Equals(regularSecurity.Currency.IsoCode, "BRL", StringComparison.InvariantCultureIgnoreCase)) 
                return resultProvider.SimpleIndexMapping("IBX50");
            else 
                return resultProvider.SimpleIndexMapping("MSCIEF");
        default:
            throw new NotImplementedException("mapping case not managed");
    }    
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// this is the standard part that shouldn't be useful to be touched
//
var savedProxies = ProcessContextStream
    .EfCoreSelect("Get securities with no proxy", (ctx, j) => ctx.Set<SecurityInstrument>().Where(security=>!security.ProxyPositions.Any()))
    .CrossApplySecurityProxyIndexPositions("Compute security proxies", i => i.Id, GetRule)
    .EfCoreSave("Save proxies", o=>o.SeekOn(i=>new {i.SecurityInstrumentId, i.IndexId}));

return ProcessContextStream.WaitWhenDone("wait till everything is done", savedProxies);
