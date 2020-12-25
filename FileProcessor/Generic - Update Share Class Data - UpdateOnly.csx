var fileDefinition = FlatFileDefinition.Create(i => new
{
    InternalCode = i.ToColumn("InternalCode"),
    Isin = i.ToColumn("Isin"),
    BloombergCode = i.ToColumn("BloombergCode"),
    SubFundCode = i.ToColumn("SubFundCode"),
    InvestorType = i.ToColumn("InvestorType"),
    DividendPolicy = i.ToColumn("DividendPolicy"),
    IsPrimary = i.ToBooleanColumn("IsPrimary","TRUE","FALSE"),
    PreviousName = i.ToColumn("PreviousName"),
    Name = i.ToColumn("Name"),
    ShortName = i.ToColumn("ShortName"),
    MgmtFee = i.ToNumberColumn<double?>("MgmtFee", "."),
    PerfFee = i.ToNumberColumn<double?>("PerfFee", "."),
    EntryFee = i.ToNumberColumn<double?>("EntryFee", "."),
    ExitFee = i.ToNumberColumn<double?>("ExitFee", "."),
    InceptionDate = i.ToOptionalDateColumn("InceptionDate","yyyy-MM-dd"),
    Srri = i.ToNumberColumn<double?>("Srri", "."),
    MinInvestment = i.ToNumberColumn<double?>("MinInvestment", "."),
    Open = i.ToBooleanColumn("Open","TRUE","FALSE"),
    MaximumSubscriptionFee = i.ToNumberColumn<double?>("MaximumSubscriptionFee", "."),
    IsHedged = i.ToBooleanColumn("IsHedged","TRUE","FALSE"),
    CcyIso = i.ToColumn("CcyIso"),
}).IsColumnSeparated(',');

var shareClassDataFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse share class data file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var existingShareClassesStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get share class stream from db", 
        (ctx, j) => ctx.Set<ShareClass>().Where(i=>i.SubFundId.HasValue));
var existingSubFundsStream = ProcessContextStream
        .EfCoreSelect($"{TaskName}: get sub fund stream from db", (ctx, j) => ctx.Set<SubFund>());

var shareClassStream = shareClassDataFileStream
    .Lookup($"{TaskName}: Get existing share class",existingShareClassesStream, i=>i.InternalCode,i=>i.InternalCode,
        (l,r) => new {FileRow = l, ExistingShareClass = r})
    .Lookup($"{TaskName}: Get existing sub fund",existingSubFundsStream, i=>i.FileRow.SubFundCode,i=>i.InternalCode,
        (l,r) => new {l.FileRow, l.ExistingShareClass, ExistingSubFund = r})
    .Where($"{TaskName}: Keep not null", i => i.ExistingShareClass!=null && i.ExistingSubFund!=null)
    .LookupCurrency($"{TaskName}: Get related currency", i=>i.FileRow.CcyIso,
        (l,r) => new {l.FileRow, l.ExistingShareClass, l.ExistingSubFund, Currency = r})
    .Select($"{TaskName}: Update share class", i => {
        i.ExistingShareClass.SubFundId = i.ExistingSubFund.Id;
        i.ExistingShareClass.ShortName = !string.IsNullOrEmpty(i.FileRow.ShortName)? i.FileRow.ShortName : i.ExistingShareClass.ShortName;
        i.ExistingShareClass.Name = !string.IsNullOrEmpty(i.FileRow.Name)? i.FileRow.Name : i.ExistingShareClass.Name;
        i.ExistingShareClass.EntryFee = i.FileRow.EntryFee;
        i.ExistingShareClass.ExitFee = i.FileRow.ExitFee;
        i.ExistingShareClass.ManagementFee = i.FileRow.MgmtFee;
        i.ExistingShareClass.PerformanceFee = i.FileRow.PerfFee;
        i.ExistingShareClass.MinimumInvestment = i.FileRow.MinInvestment;
        i.ExistingShareClass.MaximumSubscriptionFee = i.FileRow.MaximumSubscriptionFee;
        i.ExistingShareClass.InceptionDate = i.FileRow.InceptionDate;
        i.ExistingShareClass.IsHedged = i.FileRow.IsHedged;
        i.ExistingShareClass.IsOpenForInvestment = i.FileRow.Open;
        i.ExistingShareClass.CurrencyId = i.Currency != null? i.Currency.Id : i.ExistingShareClass.CurrencyId;
        i.ExistingShareClass.InvestorType = (InvestorType) Enum.Parse(typeof(InvestorType), 
                                            i.FileRow.InvestorType, true);
        i.ExistingShareClass.DividendDistributionPolicy = (DividendDistributionPolicy) 
                    Enum.Parse(typeof(DividendDistributionPolicy), i.FileRow.DividendPolicy, true);
        return i.ExistingShareClass;
    })
    .EfCoreSave($"{TaskName}: save Share Class");

var primaryShareClassFileStream = shareClassDataFileStream
    .Where($"{TaskName}: Keep primary share classes file stream", i => i.IsPrimary)
    .Lookup($"{TaskName}: lookup related existing primary Share Classes", existingShareClassesStream, 
        i => i.InternalCode, i => i.InternalCode, (l,r) => new {FileRow = l, ExistingShareClass = r})
    .Lookup($"{TaskName}: lookup related existing SubFund", existingSubFundsStream, 
        i => i.FileRow.SubFundCode, i => i.InternalCode, 
        (l,r) => new {l.FileRow, l.ExistingShareClass, ExistingSubFund = r})
    .Where($"{TaskName}: Keep not null 2", i => i.ExistingShareClass!=null && i.ExistingSubFund!=null)
    .Select($"{TaskName}: Update sub fund primary share class", i => {
        i.ExistingSubFund.PrimaryShareClassId = i.ExistingShareClass.Id;       
        return i.ExistingSubFund;
    })
    .EfCoreSave($"{TaskName}: save ExistingSubFund");

var saveBbgCode = shareClassDataFileStream
    .Distinct($"{TaskName}: distinct bloomberg code", i => i.BloombergCode)
    .Lookup($"{TaskName}: get related share class",shareClassStream, i => i.Isin, i => i.Isin,
        (l,r) => new {FileRow = l, ShareClass = r})
    .Select($"{TaskName}: create Bloomberg code", i => new SecurityDataProviderCode
    {
        SecurityId = (i.ShareClass != null)? i.ShareClass.Id : throw new Exception("ShareClass not found: "+ i.FileRow.Isin),
        Code = i.FileRow.BloombergCode,
        DataProvider = "Bloomberg",
    })
    .EfCoreSave($"{TaskName}: saveBbgCodeSave", o => o.SeekOn(i => new { i.Code, i.SecurityId, i.DataProvider }));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved", shareClassStream,
        primaryShareClassFileStream);