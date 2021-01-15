var generalParameters = ProcessContextStream.EfCoreSelect($"{TaskName}: get gp", (ctx, j) => ctx.Set<Parameters>());

var defaultCurrency = ProcessContextStream //!! USD
    .EfCoreSelect($"{TaskName}: get euroCurrency", (ctx, j) => ctx.Set<Currency>().Where(c => c.IsoCode == "USD"))
    .EnsureSingle($"{TaskName}: ensures only one euro currency");

var fixGp = generalParameters
        .Select($"{TaskName}: update data", defaultCurrency, (p,cur)=>{
                p.DefaultCurrencyId = cur.Id;
                p.MarketDataProviderConnectionSetups = new List<MarketDataProviderSetup>(){ 
                        new MarketDataProviderSetup{
                                MarketDataProviderCode = "EOD",
                                ConnectionParameterValues = new Dictionary<string, string>  { ["Token"] = "5e7ceb1fb9c9b9.58955342"}
                        }
                };
                return p;
        })
        .EfCoreSave($"{TaskName}: save updated parameters");

//RISK MANAGEMENT PARAMETERS
var riskMgmtClassificationType = ProcessContextStream 
    .EfCoreSelect($"{TaskName}: get riskMgmtClassificationType", (ctx, j) => 
        ctx.Set<ClassificationType>().Where(c => c.Code == "GICS"))
    .EnsureSingle($"{TaskName}: ensures only one riskMgmtClassificationType");

var generalParameters2 = ProcessContextStream.EfCoreSelect($"{TaskName}: get gp2", (ctx, j) => ctx.Set<Parameters>());

var fixGp2 = generalParameters
        .Select($"{TaskName}: update data2", riskMgmtClassificationType, (p,c)=>{
                p.DefaultClassificationTypeForRiskManagementId = c.Id;
                p.PricePeriodForRiskManagement = Period.ThreeYears;
                p.PositionPeriodForRiskManagement = Period.OneMonth;
                return p;
        })
        .EfCoreSave($"{TaskName}: save updated parameters2");

ProcessContextStream.WaitWhenDone("wait till everything is done", fixGp,fixGp2)