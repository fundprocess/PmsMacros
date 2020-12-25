const string classificationTypeCode = "SecurityInternalClassification1";
const string classificationTypeNameEn = "Security Internal Classification 1";
var classifications = new List<string> {"Equity", "Bond", "Sub-Fund", "ETF", "Other"};

string GetClassification(SecurityInstrument security)
{
    if (security is Equity)
        return "Equity";
    else if (security is Bond)
        return "Bond";
    else if (security is ShareClass)
        return "Sub-Fund";
    else if (security is Etf)
        return "ETF";
    else
        return "Other";
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// This is the standard part that shouldn't be useful to be touched
///////////////////////////////////////////////////////////////////////////////////////////////////

#region Create the Classification type and Classifications
var classificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create classification type", ctx => new SecurityClassificationType { 
            Code = classificationTypeCode, 
            Name = new MultiCultureString { ["en"] =classificationTypeNameEn } })
    .EfCoreSave($"{TaskName}: Save classification type", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure classification type is single");

// Create SecurityClassification
var classificationStream = ProcessContextStream
    .CrossApplyEnumerable($"{TaskName}: Cross apply input", i=> classifications)
    .Select($"{TaskName}: Get related classification type", classificationTypeStream, (i, ct) => new SecurityClassification
    {
        Code = i,
        Name = new MultiCultureString { ["en"] = System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save classification", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());
#endregion

#region Assign its classification to each target security
var classificationsOfSecurity = ProcessContextStream.EfCoreSelect("Get every security", (ctx, j) => ctx .Set<SecurityInstrument>()
    .Where(s => !s.Classifications.Any(c => c.ClassificationType.Code == classificationTypeCode)))
    .Select("Compute the classification of the security", classificationTypeStream, (s, t) => new
    {
        SecurityId = s.Id,
        ClassificationCode = GetClassification(s),
        ClassificationTypeId = t.Id
    })
    .EfCoreLookup("Get related classification Id", o => o
        .Set<SecurityClassification>()
        .On(i => i.ClassificationCode, sc => sc.Code)
        .Select((l, r) => new ClassificationOfSecurity
        {
            SecurityId = l.SecurityId,
            ClassificationId = r.Id,
            ClassificationTypeId = l.ClassificationTypeId
        }))
    .EfCoreSave("Save security classifications");
#endregion

ProcessContextStream.WaitWhenDone("wait till everything is done", classificationsOfSecurity)