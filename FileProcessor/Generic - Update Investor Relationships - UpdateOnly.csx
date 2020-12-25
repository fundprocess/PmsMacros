var fileDefinition = FlatFileDefinition.Create(i => new
{
    EntityCode = i.ToColumn("EntityCode"),
    PortfolioCode = i.ToColumn("PortfolioCode"),
    PortfolioName = i.ToColumn("PortfolioName"),    
    StatementFrequency = i.ToColumn("StatementFrequency"),
    InvestorType = i.ToColumn("InvestorType"),
    RefCcyIso = i.ToColumn("RefCcyIso"),
    StartDate = i.ToOptionalDateColumn("StartDate","yyyy-MM-dd"),
    EndDate = i.ToOptionalDateColumn("EndDate","yyyy-MM-dd"),
    PrimaryInternalAdvisorCode = i.ToColumn("PrimaryInternalAdvisorCode"),
    SecondaryInternalAdvisorCode = i.ToColumn("SecondaryInternalAdvisorCode"),
    IntermediaryCode = i.ToColumn("IntermediaryCode"),
}).IsColumnSeparated(',');

var investorRelationshipsFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse data file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var investorRelationshipsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get investors stream from db", 
        (ctx, j) => ctx.Set<InvestorRelationship>().Include(i => i.Entity));

var clientAdvisorsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get client adivsors from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i => i.Entity)
                        .Where(i=> i.Role.Domain == RoleDomain.ClientAdvisor));

var distributorsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get sales / distributors from db", (ctx, j) => 
                        ctx.Set<RoleRelationship>().Include(i => i.Entity).Include(i => i.Role)
                        .Where(i=> i.Role.Code == "Distributor"));

var investorStream = investorRelationshipsFileStream
    .Distinct($"{TaskName}: Distinct Investor entity code", i => i.EntityCode)
    .Lookup($"{TaskName}: Get related Investor DB", investorRelationshipsDbStream, i=> i.EntityCode, i=> i.Entity.InternalCode,
        (l,r) => new {FileRow = l, InvestorDb = r})
    .Where($"{TaskName}: Keep not null", i => i.InvestorDb != null)
    .LookupCurrency($"{TaskName}: Get Related Currency", i => i.FileRow.RefCcyIso,
        (l,r) => new {FileRow = l.FileRow, InvestorDb = l.InvestorDb, Currency = r})
    .Lookup($"{TaskName}: Get related primary advisor",clientAdvisorsDbStream, 
        i => i.FileRow.PrimaryInternalAdvisorCode , i=> i.Entity.InternalCode,
        (l,r) => new {FileRow = l.FileRow, InvestorDb = l.InvestorDb, Currency = l.Currency, PrimaryInternalAdvisor = r})
    .Lookup($"{TaskName}: Get related secondary advisor",clientAdvisorsDbStream, 
        i => i.FileRow.SecondaryInternalAdvisorCode, i=> i.Entity.InternalCode,
        (l,r) => new {FileRow = l.FileRow, InvestorDb = l.InvestorDb, Currency = l.Currency, 
                PrimaryInternalAdvisor = l.PrimaryInternalAdvisor, SecondaryInternalAdvisor = r})
    .Lookup($"{TaskName}: Get related intermediary", distributorsDbStream, i => i.FileRow.IntermediaryCode, i=> i.Entity.InternalCode,
        (l,r) => new {FileRow = l.FileRow, InvestorDb = l.InvestorDb, Currency = l.Currency, 
                    PrimaryInternalAdvisor = l.PrimaryInternalAdvisor,
                    SecondaryInternalAdvisor = l.SecondaryInternalAdvisor, Intermediary = r})
    .Select($"{TaskName}: Update investor relation data", i => {            
        i.InvestorDb.StartDate = i.FileRow.StartDate.HasValue? i.FileRow.StartDate : i.InvestorDb.StartDate ;
        i.InvestorDb.EndDate = i.FileRow.EndDate.HasValue? i.FileRow.EndDate : i.InvestorDb.EndDate ;
        i.InvestorDb.CurrencyId= i.Currency != null? i.Currency.Id : i.InvestorDb.CurrencyId;
        i.InvestorDb.InvestorType= !string.IsNullOrEmpty(i.FileRow.InvestorType)? 
                    (InvestorType)Enum.Parse(typeof(InvestorType), i.FileRow.InvestorType, true) : i.InvestorDb.InvestorType;
        i.InvestorDb.StatementFrequency= !string.IsNullOrEmpty(i.FileRow.StatementFrequency)? 
                    (FrequencyType)Enum.Parse(typeof(FrequencyType), i.FileRow.StatementFrequency, true): i.InvestorDb.StatementFrequency;
        i.InvestorDb.IntermediaryId = i.Intermediary != null? i.Intermediary.Id 
                        : throw new Exception($"Intermediary not found: {i.FileRow.IntermediaryCode}") ;
        i.InvestorDb.PrimaryInternalAdvisorId= i.PrimaryInternalAdvisor != null? i.PrimaryInternalAdvisor.Id 
                        : throw new Exception($"PrimaryInternalAdvisor not found: {i.FileRow.PrimaryInternalAdvisorCode}");
        i.InvestorDb.SecondaryInternalAdvisorId= i.SecondaryInternalAdvisor != null? i.SecondaryInternalAdvisor.Id 
                        : throw new Exception($"SecondaryInternalAdvisor not found: {i.FileRow.SecondaryInternalAdvisorCode}");
        return i.InvestorDb;
    })
    .EfCoreSave($"{TaskName}: save investor relationship");

//Create links between the investor relationship and the portfolios
var investorPortfolioStream = investorRelationshipsFileStream
    .Lookup($"{TaskName}: Get related Investor", investorRelationshipsDbStream, i=> i.EntityCode, i=> i.Entity.InternalCode,
        (l,r) => new {FileRow = l, InvestorDb = r})
    .LookupPortfolio($"{TaskName}: Get related portfolio", i => i.FileRow.PortfolioCode, 
        (l,r) => new {FileRow = l, InvestorDb = l.InvestorDb, Portfolio = r})
    .Where($"{TaskName}: Where Portfolio and investor not null", i => i.InvestorDb != null && i.Portfolio !=null)
    .Select($"{TaskName}: create portfolios to investor relationships", i => new RelationshipPortfolio()
    {
        PortfolioId = i.Portfolio.Id,
        RelationshipId = i.InvestorDb.Id,
    })
    .EfCoreSave($"{TaskName}: save portfolios to investor relationships", o => o
        .SeekOn(i => new {i.PortfolioId , i.RelationshipId}).DoNotUpdateIfExists());


return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", investorStream,investorPortfolioStream );