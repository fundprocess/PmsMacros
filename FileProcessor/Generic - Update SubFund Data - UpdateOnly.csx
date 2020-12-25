var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    SubFundName = i.ToColumn("SubFundName"),
    ShortName = i.ToColumn("ShortName"),
    SicavInternalCode = i.ToColumn("SicavInternalCode"),
    SicavName = i.ToColumn("SicavName"),
    FactsheetClassification = i.ToColumn("FactsheetClassification"),
    Horizon = i.ToNumberColumn<double?>("Horizon", "."),
    InvestmentProcess = i.ToColumn("InvestmentProcess"),
    SettlementNbDays = i.ToNumberColumn<int?>("SettlementNbDays", "."),
    URL = i.ToColumn("URL"),
    PricingFrequency = i.ToColumn("PricingFrequency"),
    Domicile = i.ToColumn("Domicile"),
    Country = i.ToColumn("Country"),
    CcyIso = i.ToColumn("CcyIso"),
    InceptionDate = i.ToOptionalDateColumn("InceptionDate","yyyy-MM-dd"),
    LiquidationDate = i.ToOptionalDateColumn("LiquidationDate","yyyy-MM-dd"),
    InLiquidation = i.ToOptionalBooleanColumn("InLiquidation","TRUE","FALSE"),
    CutOffTime = i.ToColumn("CutOffTime"),
    ShortExposure = i.ToOptionalBooleanColumn("ShortExposure","TRUE","FALSE"),
    ClosedEnded = i.ToOptionalBooleanColumn("ClosedEnded","TRUE","FALSE"),
    Leverage = i.ToOptionalBooleanColumn("Leverage","TRUE","FALSE"),
    SubscriptionContact = i.ToColumn("SubscriptionContact"),
    Manager1 = i.ToColumn("Manager1"),
    Manager2 = i.ToColumn("Manager2"),
    Custodian = i.ToColumn("Custodian"),
    ManCo = i.ToColumn("ManCo"),
    FundAdmin = i.ToColumn("FundAdmin"),
}).IsColumnSeparated(',');

var subFundsFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse sub fund data file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");
var subFundsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get sub fund stream from db", (ctx, j) => ctx.Set<SubFund>());

var peopleDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get people from db", (ctx, j) => ctx.Set<Person>());

var updatedSubFundstream = subFundsFileStream
    .Lookup($"{TaskName}: Get existing sub fund",subFundsDbStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {FileRow = l, SubFundDb = r})
    .Where($"{TaskName}: Keep not null", i => i.SubFundDb!=null)
    .LookupCountry($"{TaskName}: Get Related Country", i => i.FileRow.Country,
        (l,r) => new {FileRow = l.FileRow, SubFundDb = l.SubFundDb, Country = r})
    .LookupCountry($"{TaskName}: Get Related Domicile", i => i.FileRow.Domicile,
        (l,r) => new {FileRow = l.FileRow, SubFundDb = l.SubFundDb, Country = l.Country, Domicile = r})
    .LookupCurrency($"{TaskName}: Get Related Currency", i => i.FileRow.CcyIso,
        (l,r) => new {FileRow = l.FileRow, SubFundDb = l.SubFundDb, Country = l.Country, Domicile = l.Domicile, Currency = r})
    .Lookup($"{TaskName}: Get Related subscription contact", peopleDbStream, i => i.FileRow.SubscriptionContact, i => i.InternalCode,
        (l,r) => new {FileRow = l.FileRow, SubFundDb = l.SubFundDb, Country = l.Country, 
            Domicile = l.Domicile, Currency = l.Currency, SubscriptionContact = r})
    .Select($"{TaskName}: Update sub fund", i => {       
        i.SubFundDb.Name = !string.IsNullOrEmpty(i.FileRow.SubFundName)? i.FileRow.SubFundName : i.SubFundDb.Name;
        i.SubFundDb.ShortName = !string.IsNullOrEmpty(i.FileRow.ShortName)? i.FileRow.SubFundName : i.SubFundDb.ShortName; 
        i.SubFundDb.RecommendedTimeHorizon = i.FileRow.Horizon.HasValue? i.FileRow.Horizon.Value : i.SubFundDb.RecommendedTimeHorizon; 
        i.SubFundDb.InvestmentProcess = (InvestmentProcessType) Enum.Parse(typeof(InvestmentProcessType), i.FileRow.InvestmentProcess, true);  
        i.SubFundDb.SettlementNbDays = i.FileRow.SettlementNbDays.HasValue? i.FileRow.SettlementNbDays.Value : i.SubFundDb.SettlementNbDays;
        i.SubFundDb.Url = !string.IsNullOrEmpty(i.FileRow.URL)? i.FileRow.URL : i.SubFundDb.Url;
        i.SubFundDb.PricingFrequency = (FrequencyType) Enum.Parse(typeof(FrequencyType), i.FileRow.PricingFrequency, true); 
        i.SubFundDb.DomicileId = i.Domicile != null ? i.Domicile.Id : (int?) null;
        i.SubFundDb.CountryId = i.Country != null ? i.Country.Id : (int?) null;
        i.SubFundDb.CurrencyId = i.Currency != null ? i.Currency.Id : (int?) null;
        i.SubFundDb.InceptionDate = i.FileRow.InceptionDate !=null? i.FileRow.InceptionDate : i.SubFundDb.InceptionDate; 
        i.SubFundDb.LiquidationDate = i.FileRow.LiquidationDate !=null? i.FileRow.LiquidationDate : i.SubFundDb.LiquidationDate; 
        i.SubFundDb.InLiquidation = i.FileRow.InLiquidation !=null? i.FileRow.InLiquidation.Value : i.SubFundDb.InLiquidation;
        i.SubFundDb.CutOffTime = !string.IsNullOrEmpty(i.FileRow.CutOffTime)?
                            (TimeSpan.TryParse(i.FileRow.CutOffTime,out var res)?res: i.SubFundDb.CutOffTime):i.SubFundDb.CutOffTime; 
        i.SubFundDb.ShortExposure = i.FileRow.ShortExposure !=null? i.FileRow.ShortExposure.Value : i.SubFundDb.ShortExposure;
        i.SubFundDb.ClosedEnded = i.FileRow.ClosedEnded !=null? i.FileRow.ClosedEnded.Value : i.SubFundDb.ClosedEnded;
        i.SubFundDb.Leverage = i.FileRow.Leverage !=null? i.FileRow.Leverage.Value : i.SubFundDb.Leverage;
        i.SubFundDb.SubscriptionContactId = i.SubscriptionContact != null? i.SubscriptionContact.Id : (int?) null;
        return i.SubFundDb;
    })
    .EfCoreSave($"{TaskName}: save sub fund");

#region roles
var primaryManagersDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get primary managers from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i=>i.Role).Include(i=>i.Entity)
                        .Where(i=> i.Role.Code == "PrimaryPortfolioManager"));
var managers1Stream = subFundsFileStream
    .Distinct($"{TaskName}: Distinct Sub fund internal code", i=>i.InternalCode)
    .Lookup($"{TaskName}: lookup primary manager", primaryManagersDbStream, i => i.Manager1, i => i.Entity.InternalCode,
        (l,r) => new {FileRow = l, Manager1 = r})
    .LookupPortfolio($"{TaskName}: Get related sub fund", i=>i.FileRow.InternalCode,
        (l,r) => new {FileRow = l.FileRow, Manager1 = l.Manager1, SubFundDb = r})
    .Where($"{TaskName}: Keep not null 2", i => i.SubFundDb != null)
    .Select($"{TaskName}: create link manager1", i => new RelationshipPortfolio()
    {
        PortfolioId = (i.SubFundDb != null)? i.SubFundDb.Id : throw new Exception("Sub Fund not found: " + i.FileRow.InternalCode),
        RelationshipId = (i.Manager1 != null)? i.Manager1.Id : throw new Exception("Manager 1 not found: " + i.FileRow.Manager1),
    })
    .EfCoreSave($"{TaskName}: save link manager1", o => o
        .SeekOn(i => new {i.PortfolioId , i.RelationshipId}).DoNotUpdateIfExists() );

var secondaryManagersDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get secondary managers from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i=>i.Role).Include(i=>i.Entity)
                        .Where(i=> i.Role.Code == "SecondaryPortfolioManager"));
var managers2Stream = subFundsFileStream
    .Distinct($"{TaskName}: Distinct Sub fund internal code for secondary manager", i=>i.InternalCode)
    .Lookup($"{TaskName}: lookup secondary manager", secondaryManagersDbStream, i => i.Manager2, i => i.Entity.InternalCode,
        (l,r) => new {FileRow = l, Manager2 = r})
    .LookupPortfolio($"{TaskName}: Get related sub fund for secondary manager", i=>i.FileRow.InternalCode,
        (l,r) => new {FileRow = l.FileRow, Manager2 = l.Manager2, SubFundDb = r})
    .Where($"{TaskName}: Keep sub fund not null for secondary manager", i => i.SubFundDb != null)
    .Select($"{TaskName}: create link manager2", i => new RelationshipPortfolio()
    {
        PortfolioId = (i.SubFundDb != null)? i.SubFundDb.Id : throw new Exception("Sub Fund not found: " + i.FileRow.InternalCode),
        RelationshipId = (i.Manager2 != null)? i.Manager2.Id : throw new Exception("Manager 2 not found: " + i.FileRow.Manager2),
    })    
    .EfCoreSave($"{TaskName}: save link manager2", o => o
        .SeekOn(i => new {i.PortfolioId , i.RelationshipId}).DoNotUpdateIfExists());

var manCosDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get manCos from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i=>i.Role).Include(i=>i.Entity)
                        .Where(i=> i.Role.Code == "ManagementCompany"));
var manCosStream = subFundsFileStream
    .Distinct($"{TaskName}: Distinct Sub fund internal code for manco", i=>i.InternalCode)
    .Lookup($"{TaskName}: lookup manco", manCosDbStream, i => i.ManCo, i => i.Entity.InternalCode,
        (l,r) => new {FileRow = l, Manco = r})
    .LookupPortfolio($"{TaskName}: Get related sub fund for manco", i=>i.FileRow.InternalCode,
        (l,r) => new {FileRow = l.FileRow, Manco = l.Manco, SubFundDb = r})
    .Where($"{TaskName}: Keep sub fund not null for manco", i => i.SubFundDb != null)
    .Select($"{TaskName}: create link Manco", i => new RelationshipPortfolio()
    {
        PortfolioId = (i.SubFundDb != null)? i.SubFundDb.Id : throw new Exception("Sub Fund not found: " + i.FileRow.InternalCode),
        RelationshipId = (i.Manco != null)? i.Manco.Id : throw new Exception("ManCo not found: " + i.FileRow.ManCo) ,
    })    
    .EfCoreSave($"{TaskName}: save link Manco", o => o
        .SeekOn(i => new {i.PortfolioId , i.RelationshipId}).DoNotUpdateIfExists());

var custodiansDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get Custodians from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i=>i.Role).Include(i=>i.Entity)
                        .Where(i=> i.Role.Code == "DepositaryBank"));
var custodiansStream = subFundsFileStream
    .Distinct($"{TaskName}: Distinct Sub fund internal code for Custodian", i=>i.InternalCode)
    .Lookup($"{TaskName}: lookup Custodian", custodiansDbStream, i => i.Custodian, i => i.Entity.InternalCode,
        (l,r) => new {FileRow = l, Custodian = r})
    .LookupPortfolio($"{TaskName}: Get related sub fund for Custodian", i=>i.FileRow.InternalCode,
        (l,r) => new {FileRow = l.FileRow, Custodian = l.Custodian, SubFundDb = r})
    .Where($"{TaskName}: Keep sub fund not null for Custodian", i => i.SubFundDb != null)
    .Select($"{TaskName}: create link Custodian", i => new RelationshipPortfolio()
    {
        PortfolioId = (i.SubFundDb != null)? i.SubFundDb.Id : throw new Exception("Sub Fund not found: " + i.FileRow.InternalCode),
        RelationshipId = (i.Custodian != null)? i.Custodian.Id : throw new Exception("Custodian not found: " + i.FileRow.Custodian) ,
    })    
    .EfCoreSave($"{TaskName}: save link Custodian", o => o
        .SeekOn(i => new {i.PortfolioId , i.RelationshipId}).DoNotUpdateIfExists());
        
var fundAdminsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get fundAdmins from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i=>i.Role).Include(i=>i.Entity)
                        .Where(i=> i.Role.Code == "FundAdmin"));
var fundAdminsStream = subFundsFileStream
    .Distinct($"{TaskName}: Distinct Sub fund internal code for fundAdmin", i=>i.InternalCode)
    .Lookup($"{TaskName}: lookup fundAdmin", fundAdminsDbStream, i => i.FundAdmin, i => i.Entity.InternalCode,
        (l,r) => new {FileRow = l, FundAdmin = r})
    .LookupPortfolio($"{TaskName}: Get related sub fund for fundAdmin", i=>i.FileRow.InternalCode,
        (l,r) => new {FileRow = l.FileRow, FundAdmin = l.FundAdmin, SubFundDb = r})
    .Where($"{TaskName}: Keep sub fund not null for fundAdmin", i => i.SubFundDb != null)
    .Select($"{TaskName}: create link fundAdmin", i => new RelationshipPortfolio()
    {
        PortfolioId = (i.SubFundDb != null)? i.SubFundDb.Id : throw new Exception("Sub Fund not found: " + i.FileRow.InternalCode),
        RelationshipId = (i.FundAdmin != null)? i.FundAdmin.Id : throw new Exception("fundAdmin not found: " + i.FileRow.FundAdmin) ,
    })    
    .EfCoreSave($"{TaskName}: save link fundAdmin", o => o
        .SeekOn(i => new {i.PortfolioId , i.RelationshipId}).DoNotUpdateIfExists());

#endregion

#region FactsheetClassification
// Create SecurityClassificationType
var classificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create Factsheet classification type", 
        ctx => new SecurityClassificationType { Code = "FactsheetCustom1", Name = new MultiCultureString { ["en"] = "Factsheet Sub Fund Classification" } })
    .EfCoreSave($"{TaskName}: Save Factsheet classification type", o => o.SeekOn(ct => ct.Code))
    .EnsureSingle($"{TaskName}: Ensure Factsheet classification type is single");

// Create SecurityClassification
var classificationStream = subFundsFileStream
    .Distinct($"{TaskName}: Distinct classification", i => i.FactsheetClassification)
    .Select($"{TaskName}: Get related classification type", classificationTypeStream, (i, ct) => new SecurityClassification
    {
        Code = i.FactsheetClassification,
        Name = new MultiCultureString { ["en"] = System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.FactsheetClassification.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save FactsheetClassification classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }));

// Classification Of Subfund assignation
var classificationOfSecurityStream = updatedSubFundstream
    .CorrelateToSingle($"{TaskName}: Get related sub fund classification", classificationStream, 
        (s, c) => new ClassificationOfPortfolio { ClassificationTypeId = c.ClassificationTypeId, PortfolioId = s.Id, ClassificationId = c.Id })
    .EfCoreSave($"{TaskName}: Insert sub fund classification", o => o.SeekOn(i => new { i.PortfolioId, i.ClassificationTypeId }).DoNotUpdateIfExists());
#endregion

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", 
    updatedSubFundstream,
    managers1Stream,
    managers2Stream,
    manCosStream,custodiansStream, fundAdminsStream,
    classificationTypeStream,classificationStream,classificationOfSecurityStream
    );