var realTimeFxRatesStream = ProcessContextStream.EfCoreSelect($"{TaskName} Get RealTime Fx Rates", (ctx,j) =>
    ctx.Set<FxRateRealTime>().Include(i => i.CurrencyTo));

var pricingPolicyStream = ProcessContextStream.EfCoreSelect($"{TaskName} Get Pricing Policy", (ctx,j) =>
    ctx.Set<PricingPolicy>()).EnsureSingle($"Ensure only one pricing policy set");

var saveFxRatesCopyStream = realTimeFxRatesStream
    .Select($"{TaskName}: Create Pricing Policy FxRates",pricingPolicyStream, (fx, pp) => new PricingPolicyFxRate{
        PricingPolicyId = pp.Id,
        CurrencyToId = fx.CurrencyToId,
        Date = fx.DateTime.Date,
        ActualDateTime = fx.DateTime,
        RateFromReferenceCurrency = fx.RateFromReferenceCurrency
    })
    .EfCoreSave($"{TaskName}: Save FxRates Duplicates", o=>o
        .SeekOn(i => new {i.PricingPolicyId,i.CurrencyToId,i.Date})); 

ProcessContextStream.WaitWhenDone("wait till everything is done",saveFxRatesCopyStream);