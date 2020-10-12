string GetClassification(SecurityInstrument security)
{
    if (security.Name.Contains("a", StringComparison.InvariantCultureIgnoreCase))
    {
        return "CLS1";
    }
    else if (security.Name.Contains("u", StringComparison.InvariantCultureIgnoreCase))
    {
        return "CLS2";
    }
    return "CLS3";
}

const string ClassificationType = "TST_CLASS";

///////////////////////////////////////////////////////////////////////////////////////////////////
// this is the standard part that shouldn't be useful to be touched
//
var tstClassificationType = ProcessContextStream.EfCoreSelect("Get tst classification type", i => i
    .Set<SecurityClassificationType>()
    .Where(ct => ct.Code == ClassificationType))
    .EnsureSingle($"Ensure only one {ClassificationType} classification type exists");

var classificationsOfSecurity = ProcessContextStream.EfCoreSelect("Get every security", i => i
    .Set<SecurityInstrument>()
    .Where(s => !s.Classifications.Any(c => c.ClassificationType.Code == ClassificationType)))
    .Select("Compute the classification of the security", tstClassificationType, (s, t) => new
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
        })
        .CacheFullDataset())
    .EfCoreSave("Save security classifications");

ProcessContextStream.WaitWhenDone("wait till everything is done", classificationsOfSecurity)