var tab = NewList("MSCI Constituents - Missing price securities");

var securitiesDic = Referential.Securities.ToDictionary(i=>i.Id, i=>i);
int inc = 0;
var rows = Referential.BenchmarkLastPositions
            .Where(i=>i.SecurityId != null && securitiesDic[i.SecurityId.Value].Price == null  )
            .Select(i => NewListRow(securitiesDic[i.SecurityId.Value].Name , new[] {
            NewValue("No","No").SetNumber(inc++),    
            NewValue("ISIN","ISIN").SetText(securitiesDic[i.SecurityId.Value].Isin),
            NewValue("Ccy","Ccy").SetText(securitiesDic[i.SecurityId.Value].CcyIso),
            NewValue("Weight","Weight").SetPercentage(i.Weight),
            NewValue("Date","Date").SetDate(i.Date),
            securitiesDic[i.SecurityId.Value].PriceDate != null ?
                NewValue("PriceDate","PriceDate").SetDate(securitiesDic[i.SecurityId.Value].PriceDate)
                :NewValue("PriceDate","PriceDate").SetText("-"),
            
            securitiesDic[i.SecurityId.Value].Price != null ?
                NewValue("Price","Price").SetNumber(securitiesDic[i.SecurityId.Value].Price)
                :NewValue("Price","Price").SetText("-"),
        })).ToList();
tab.AddRows(rows);

tab