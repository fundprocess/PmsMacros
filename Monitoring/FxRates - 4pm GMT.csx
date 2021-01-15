var tab = NewList("FxRates - 4pm GMT");

var rows = Referential.PricingPolicyLastFxRates.Select(i => NewListRow(i.CcyToIso, new[] {
            //NewValue("Date","Date").SetDate(i.Date),
            NewValue("Date Time","ActualDateTime").SetText(i.ActualDateTime.ToString("yyyy-MM-dd HH:mm:ss")),
            NewValue("Rate","RateFromReferenceCurrency").SetText(i.RateFromReferenceCurrency.ToString("0.0000"))
        })).ToList();
tab.AddRows(rows);

tab