HashSet<string> instrumentCategoryCash = new HashSet<string>(new[] { "tres", "cpon", "agde", "avfi", "capi", "char", "cmov", "comp", "cpcr", "crne", "devi", "div", "fcha", "fis ", "frai", "indi", "lat", "matp", "opcl", "part", "port", "prod", "reme", "rev", "rtro", "sic", "sr", "swac", "taux", "ters", "tisl", "tota", "tree", "trev", "trs", "vfnm", "zgat", "zgde", "ztrd" }, StringComparer.InvariantCultureIgnoreCase);
HashSet<string> subCategory2CashIfVmob = new HashSet<string>(new[] { "152319", "131005", "999999", "122050", "112007", "131013", "115500", "115000", "131010" }, StringComparer.InvariantCultureIgnoreCase);

var efaPositionFileDefinition = FlatFileDefinition.Create(i => new
{
    FundCode = i.ToColumn<string>("Fund_code"),
    FundLongName = i.ToColumn<string>("Fund_Long_Name"),
    SubFundCode = i.ToColumn<string>("Sub-Fund_Code"),
    SubFundLongName = i.ToColumn<string>("Sub-Fund_long_name"),
    SubFundCcy = i.ToColumn<string>("Sub-Fund_ccy"),
    EcoSectCode = i.ToColumn<string>("Eco_Sect_Code"),
    ValuationDate = i.ToDateColumn("Valuation_date", "dd/MM/yyyy"),
    MaturityDate = i.ToOptionalDateColumn("Maturity_date", "dd/MM/yyyy"),

    CostInInstrCcy = i.ToNumberColumn<double?>("Cost_in_Instr_CCY", "."),
    Cost = i.ToNumberColumn<double?>("Cost", "."),
    UnreaOnStockExch = i.ToNumberColumn<double?>("Unrea_on_Stock_Exch", "."),
    UnreaResultOnFx = i.ToNumberColumn<double?>("Unrea_Result_on_FX", "."),
    NumberOfAccruedDays = i.ToNumberColumn<int?>("Number_of_Accrued_Days", "."),
    InterestAccruedInInstrCcy = i.ToNumberColumn<double?>("Interest_Accrued_in_Instr_CCY", "."),
    AccruedInterest = i.ToNumberColumn<double?>("Accrued_Interest", "."),
    Nominal = i.ToNumberColumn<double?>("Nominal", "."),

    GeoSectCode = i.ToColumn<string>("Geo_sect_code"),
    InstrCode = i.ToColumn<string>("Instr_code"),
    InstrCategory = i.ToColumn<string>("Instr_Category"),
    InstrEvaluationCcy = i.ToColumn<string>("Instr_evaluation_ccy"),
    InstrLongName = i.ToColumn<string>("Instr_long_name"),
    Isin = i.ToColumn<string>("ISIN"),
    BloombergCode = i.ToColumn<string>("Bloomberg Code"),
    CouponPeriodicity = i.ToNumberColumn<int?>("Coupon_periodicity", "."),
    Category1 = i.ToColumn<string>("Category_1"),
    Category2 = i.ToColumn<string>("Category_2"),
    MarketValue = i.ToNumberColumn<double>("Market_Value", "."),
    MarketValueInInstrCcy = i.ToNumberColumn<double>("Market_Value_in_Instr_CCY", "."),
    Quantity = i.ToNumberColumn<double>("Quantity", "."),
    FilePath = i.ToSourceName(),
    LineStatus = i.ToColumn("Line_status")
}).IsColumnSeparated(',');

var posFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse position file", efaPositionFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var sicavStream = posFileStream
    .Distinct($"{TaskName}: distinct sicavs", i => i.FundCode, true)
    .Select($"{TaskName}: create sicav", ProcessContextStream, (i, ctx) => new Sicav
    {
        InternalCode = i.FundCode,
        Name = i.FundLongName,
        IssuerId = ctx.TenantId
    })
    .EfCoreSave($"{TaskName}: save sicav", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

var portfolioStream = posFileStream
    .Distinct($"{TaskName}: distinct funds", i => i.SubFundCode, true)
    .LookupCurrency($"{TaskName}: get related currency for fund", l => l.SubFundCcy, (l, r) => new { FileRow = l, Currency = r })
    .CorrelateToSingle($"{TaskName}: lookup related sicav", sicavStream, (l, r) => new { l.FileRow, l.Currency, Sicav = r })
    .Select($"{TaskName}: create fund", ProcessContextStream, (i, ctx) => (Portfolio)new SubFund
    {
        InternalCode = i.FileRow.SubFundCode,
        SicavId = i.Sicav.Id,
        Name = i.FileRow.SubFundLongName,
        ShortName = i.FileRow.SubFundLongName.Truncate(MaxLengths.ShortName),
        CurrencyId = i.Currency?.Id,
        PricingFrequency = FrequencyType.Daily
    })
    .EfCoreSave($"{TaskName}: save sub fund", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists());

// SecurityClassificationType1
var classificationType1Stream = ProcessContextStream
    .Select($"{TaskName}: Create EFA classification type 1", ctx => new SecurityClassificationType { Code = "EFA1", Name = new MultiCultureString { ["en"] = "EFA Classification 1" } })
    .EfCoreSave($"{TaskName}: Save EFA classification type 1", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure EFA classification type 1 is single");

// SecurityClassification1
var classification1Stream = posFileStream
    .Distinct($"{TaskName}: Distinct classification 1", i => i.Category1)
    .Select($"{TaskName}: Get related classification type 1", classificationType1Stream, (i, ct) => new SecurityClassification
    {
        Code = i.Category1,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.Category1.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save EFA classification 1", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// SecurityClassificationType2
var classificationType2Stream = ProcessContextStream
    .Select($"{TaskName}: Create EFA classification type 2", ctx => new SecurityClassificationType { Code = "EFA2", Name = new MultiCultureString { ["en"] = "EFA Classification 2" } })
    .EfCoreSave($"{TaskName}: Save EFA classification type 2", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure EFA classification type 2 is single");

// SecurityClassification2
var classification2Stream = posFileStream
    .Distinct($"{TaskName}: Distinct classification 2", i => i.Category2)
    .Select($"{TaskName}: Get related classification type 2", classificationType2Stream, (i, ct) => new SecurityClassification
    {
        Code = i.Category2,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.Category2.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save EFA classification 2", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

// eco sector code ClassificationType
var ecoSectorClassificationTypeStream = ProcessContextStream
    .Select($"{TaskName}: Create EFA sector code", ctx => new SecurityClassificationType { Code = "EFA_SEC", Name = new MultiCultureString { ["en"] = "EFA Sector Code" } })
    .EfCoreSave($"{TaskName}: Save EFA sector code classification type 2", o => o.SeekOn(ct => ct.Code).DoNotUpdateIfExists())
    .EnsureSingle($"{TaskName}: Ensure EFA sector code classification type is single");

// eco sector code Classification
var ecoSectorClassificationStream = posFileStream
    .Distinct($"{TaskName}: Distinct sector code classification", i => i.EcoSectCode)
    .Select($"{TaskName}: Get related sector code classification type", ecoSectorClassificationTypeStream, (i, ct) => new SecurityClassification
    {
        Code = i.EcoSectCode,
        Name = new MultiCultureString { ["en"] = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(i.EcoSectCode.ToLower()) },
        ClassificationTypeId = ct.Id
    })
    .EfCoreSave($"{TaskName}: Save EFA sector code sector code classification 2", o => o.SeekOn(ct => new { ct.ClassificationTypeId, ct.Code }).DoNotUpdateIfExists());

var targetCashStream = posFileStream
    .Where($"{TaskName}: keep cash only", i => IsCash(i.InstrCategory, i.Category1, i.Category2))
    .Select($"{TaskName}: Compute cash code", i => new
    {
        SecurityCode = $"{i.SubFundCode}-{i.InstrCode}-{i.InstrEvaluationCcy}",
        FileRow = i
    })
    .Distinct($"{TaskName}: distinct positions cash", i => i.SecurityCode)
    .LookupCountry($"{TaskName}: get related country for cash", l => l.FileRow.GeoSectCode, (l, r) => new { l.FileRow, l.SecurityCode, Country = r })
    .LookupCurrency($"{TaskName}: get related currency for cash", l => l.FileRow.InstrEvaluationCcy, (l, r) => new { l.FileRow, l.SecurityCode, l.Country, Currency = r })
    .Select($"{TaskName}: create cash", i => CreateSecurityForComposition(
            i.FileRow.InstrCategory,
            i.FileRow.Category1,
            i.FileRow.Category2,
            i.SecurityCode,
            i.FileRow.InstrLongName,
            null,
            i.Currency?.Id,
            i.FileRow.MaturityDate,
            i.Country?.Id,
            i.FileRow.Nominal,
            i.FileRow.CouponPeriodicity))
    .EfCoreSave($"{TaskName}: save target cash", o => o.SeekOn(i => i.InternalCode).DoNotUpdateIfExists())
    .Select($"{TaskName}: cast cash into Security", i => i as Security);

var targetInstrumentStream = posFileStream
    .Where($"{TaskName}: keep non cash only", i => !IsCash(i.InstrCategory, i.Category1, i.Category2))
    .Fix($"{TaskName}: recompute isin", i => i.FixProperty(p => p.Isin).AlwaysWith(p => p.Isin == "-1" ? null : p.Isin))
    .ReKey($"{TaskName}: Uniformize target instrument codes", i => new { i.Isin, i.InstrCode })
    .Distinct($"{TaskName}: distinct positions security", i => new { i.Isin, i.InstrCode })
    .LookupCountry($"{TaskName}: get related country for target security", l => l.FileRow.GeoSectCode, (l, r) => new { l.FileRow, Country = r })
    .LookupCurrency($"{TaskName}: get related currency for target security", l => l.FileRow.InstrEvaluationCcy, (l, r) => new { l.FileRow, l.Country, Currency = r })
    .Select($"{TaskName}: create instrument", i => CreateSecurityForComposition(
            i.FileRow.InstrCategory,
            i.FileRow.Category1,
            i.FileRow.Category2,
            i.FileRow.InstrCode,
            i.FileRow.InstrLongName,
            i.FileRow.Isin,
            i.Currency?.Id,
            i.FileRow.MaturityDate,
            i.Country?.Id,
            i.FileRow.Nominal,
            i.FileRow.CouponPeriodicity) as SecurityInstrument)
    .EfCoreSave($"{TaskName}: save target instrument", o => o.SeekOn(i => i.InternalCode).AlternativelySeekOn(i => i.Isin).DoNotUpdateIfExists())
    .Select($"{TaskName}: cast instrument into Security", i => i as Security);

// ClassificationOfSecurity
var classification1OfSecurityStream = targetInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related security classification 1", classification1Stream, (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });
var classification2OfSecurityStream = targetInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related security classification 2", classification2Stream, (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });
var ecoSectorOfSecurityStream = targetInstrumentStream
    .CorrelateToSingle($"{TaskName}: Get related eco sector", ecoSectorClassificationStream, (s, c) => new ClassificationOfSecurity { ClassificationTypeId = c.ClassificationTypeId, SecurityId = s.Id, ClassificationId = c.Id });

var classificationOfSecurityStream = classification1OfSecurityStream
    .Union($"{TaskName}: merge classification 1 & 2 of security", classification2OfSecurityStream)
    .Union($"{TaskName}: merge classification 1 & 2 of security & eco sector code", ecoSectorOfSecurityStream)
    .EfCoreSave($"{TaskName}: Insert classification of security", o => o.SeekOn(i => new { i.SecurityId, i.ClassificationTypeId }).DoNotUpdateIfExists());

var targetSecurityStream = targetCashStream
    .Union($"{TaskName}: merge cash and target securities", targetInstrumentStream);

var portfolioCompositionStream = posFileStream
    .Distinct($"{TaskName}: distinct composition for a date", i => new { i.SubFundCode, i.ValuationDate }, true)
    .CorrelateToSingle($"{TaskName}: get composition portfolio", portfolioStream, (l, r) => new PortfolioComposition { Date = l.ValuationDate, PortfolioId = r.Id })
    .EfCoreSave($"{TaskName}: save composition", o => o.SeekOn(i => new { i.PortfolioId, i.Date }));

var positionStream = posFileStream
    .CorrelateToSingle($"{TaskName}: get related security for position", targetSecurityStream, (l, r) => new { FileRow = l, Security = r })
    .CorrelateToSingle($"{TaskName}: get related composition for position", portfolioCompositionStream, (l, r) => new { l.FileRow, l.Security, Composition = r })
    .Aggregate($"{TaskName}: sum positions duplicates within a file",
        i => new
        {
            i.FileRow.FilePath,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id
        },
        i => new
        {
            i.FileRow,
            CompositionId = i.Composition.Id,
            SecurityId = i.Security.Id,
            Values = new
            {
                Quantity = (double)0,
                MarketValue = (double)0,
                MarketValueInInstrCcy = (double)0,
                Cost = (double)0,
                CostInInstrCcy = (double)0,
                UnreaResultOnFx = (double)0,
                UnreaOnStockExch = (double)0,
                // NumberOfAccruedDays = (int)0,
                AccruedInterest = (double)0,
                InterestAccruedInInstrCcy = (double)0,
                Count = 0
            }
        },
        (a, v) =>
        new
        {
            v.FileRow,
            CompositionId = v.Composition.Id,
            SecurityId = v.Security.Id,
            Values = new
            {
                Quantity = a.Values.Quantity + v.FileRow.Quantity,
                MarketValue = a.Values.MarketValue + v.FileRow.MarketValue,
                MarketValueInInstrCcy = a.Values.MarketValueInInstrCcy + v.FileRow.MarketValueInInstrCcy,
                Cost = a.Values.Cost + v.FileRow.Cost ?? 0,
                CostInInstrCcy = a.Values.CostInInstrCcy + v.FileRow.CostInInstrCcy ?? 0,
                UnreaResultOnFx = a.Values.UnreaResultOnFx + v.FileRow.UnreaResultOnFx ?? 0,
                UnreaOnStockExch = a.Values.UnreaOnStockExch + v.FileRow.UnreaOnStockExch ?? 0,
                // NumberOfAccruedDays = a.Values.NumberOfAccruedDays + v.FileRow.NumberOfAccruedDays ?? 0,
                AccruedInterest = a.Values.AccruedInterest + v.FileRow.AccruedInterest ?? 0,
                InterestAccruedInInstrCcy = a.Values.InterestAccruedInInstrCcy + v.FileRow.InterestAccruedInInstrCcy ?? 0,
                Count = a.Values.Count + 1,
            }
        })
    .Select($"{TaskName}: get position aggregation",
        i =>
        new
        {
            CompositionId = i.Aggregation.CompositionId,
            SecurityId = i.Aggregation.SecurityId,
            NumberOfAccruedDays = i.Aggregation.FileRow.NumberOfAccruedDays,
            Values = new
            {
                Quantity = i.Aggregation.Values.Quantity / i.Aggregation.Values.Count,
                i.Aggregation.Values.MarketValue,
                i.Aggregation.Values.Cost,
                i.Aggregation.Values.CostInInstrCcy,
                i.Aggregation.Values.MarketValueInInstrCcy,
                i.Aggregation.Values.UnreaResultOnFx,
                i.Aggregation.Values.UnreaOnStockExch,
                // NumberOfAccruedDays = (int)i.Aggregation.Values.NumberOfAccruedDays / i.Aggregation.Values.Count,
                i.Aggregation.Values.AccruedInterest,
                i.Aggregation.Values.InterestAccruedInInstrCcy,
            }
        })
    .Distinct($"{TaskName}: exclude positions duplicates", i => new { i.CompositionId, i.SecurityId }, true)
    .Select($"{TaskName}: create position", i => new Position
    {
        SecurityId = i.SecurityId,
        MarketValueInPortfolioCcy = i.Values.MarketValue,
        MarketValueInSecurityCcy = i.Values.MarketValueInInstrCcy,
        PortfolioCompositionId = i.CompositionId,
        Value = i.Values.Quantity,
        BookCostInPortfolioCcy = i.Values.Cost,
        BookCostInSecurityCcy = i.Values.CostInInstrCcy,
        ProfitLossOnFxPortfolioCcy = i.Values.UnreaResultOnFx,
        ProfitLossOnMarketPortfolioCcy = i.Values.UnreaOnStockExch,
        NbAccruedDays = i.NumberOfAccruedDays,
        AccruedInterestInPortfolioCcy = i.Values.AccruedInterest,
        AccruedInterestInSecurityCcy = i.Values.InterestAccruedInInstrCcy
    })
    .ComputeWeight(TaskName)
    .EfCoreSave($"{TaskName}: save position", o => o.SeekOn(i => new { i.SecurityId, i.PortfolioCompositionId }));

return FileStream.WaitWhenDone($"{TaskName}: wait till every position is saved", positionStream, classificationOfSecurityStream);



bool IsCash(string instrumentCategory, string category1, string category2)
{
    if (instrumentCategoryCash.Contains(instrumentCategory)) return true;
    else if (string.Equals(instrumentCategory, "vmob", StringComparison.InvariantCultureIgnoreCase)) return subCategory2CashIfVmob.Contains(category2);
    else return false;
}
FrequencyType? MapPeriodicity(int? couponPeriodicity)
{
    if (couponPeriodicity == null) return null;
    switch (couponPeriodicity.Value)
    {
        case 10:
        case 365:
        case 99:
        case 999: return FrequencyType.Daily;
        case 1: return FrequencyType.Yearly;
        case 12: return FrequencyType.Monthly;
        case 3: return FrequencyType.ThirdYearly;
        case 2: return FrequencyType.HalfYearly;
        case 4: return FrequencyType.Quarterly;
        case 6: return FrequencyType.HalfMonthly;
        default: return null;
    }
}
Security CreateSecurityForComposition(string instrumentCategory, string category1, string category2, string instrumentCode, string instrumentName, string instrumentIsin, int? currencyId, DateTime? maturityDate, int? countryId, double? nominal, int? couponPeriodicity)
{
    Security security = null;
    switch (instrumentCategory.ToLower())
    {
        case "tres":
        case "cpon":
        case "agde":
        case "avfi":
        case "capi":
        case "char":
        case "cmov":
        case "comp":
        case "cpcr":
        case "crne":
        case "devi":
        case "div":
        case "fcha":
        case "fis ":
        case "frai":
        case "indi":
        case "lat":
        case "matp":
        case "opcl":
        case "part":
        case "port":
        case "prod":
        case "reme":
        case "rev":
        case "rtro":
        case "sic":
        case "sr":
        case "swac":
        case "taux":
        case "ters":
        case "tisl":
        case "tota":
        case "tree":
        case "trev":
        case "trs":
        case "vfnm":
        case "zgat":
        case "zgde":
        case "ztrd":
            security = new Cash();
            break;
        case "opti":
            security = new Option { Type = OptionType.European }; //TODO: Maybe see if this can be more generic
            break;
        case "futu":
            security = new Future();
            break;
        case "swat":
            security = new Swap();
            break;
        case "cat":
            security = new FxForward();
            break;
        case "vmob":
            security = CreateSecurityForCompositionIfVmob(category1, category2);
            switch (security)
            {
                case OptionFuture derivative:
                    derivative.UnderlyingIsin = instrumentIsin;
                    break;
            }
            break;
        case "cfd":
            security = new Cfd();
            break;
            //case "abnp":
            //case "abtp":
            //case "all":
            //case "delta":
            //case "dpt":
            //case "emet":
            //case "frt":
            //case "iism":
            //case "iiso":
            //case "iisp":
            //case "opc":
            //case "swar":
            //case "swav":
            //case "tie":
            //case "trew":
            //    security = null;
            //    break;
    }

    if (security != null)
    {
        security.InternalCode = instrumentCode;
        security.CurrencyId = currencyId;
        if (security is Derivative der)
            der.MaturityDate = maturityDate;
        if (security is StandardDerivative standardDerivative)
            standardDerivative.Nominal = nominal;
        if (security is OptionFuture optFut)
            optFut.UnderlyingIsin = instrumentIsin;
        else if (security is SecurityInstrument securityInstrument)
            securityInstrument.Isin = instrumentIsin;
        if (security is Bond bond)
        {
            bond.CouponFrequency = MapPeriodicity(couponPeriodicity);
        }
        security.Name = instrumentName;
        security.ShortName = instrumentName.Truncate(MaxLengths.ShortName);
    }
    if (security is RegularSecurity regularSecurity)
    {
        regularSecurity.CountryId = countryId;
    }
    return security;
}
Security CreateSecurityForCompositionIfVmob(string subCategory1, string subCategory2)
{
    switch (subCategory2.ToLower())
    {
        case "112010": //Asset_backed_securities
        case "112000": //bonds
        case "112009": //certificate_of_negociable_deposit
        case "111953": //equity_linked_notes
        case "112953": //equity_linked_notes_2
        case "119953": //equity_linked_notes_3
        case "112012": //euro_medium_term_notes
        case "112014": //medium_term_certificates
        case "113000": //money_market_instruments
        case "112099": //other_bonds
        case "112008": //reverse_convertible_notes
        case "112011": //treasury_bills
        case "112013": //treasury_bonds
        case "111690": //certificats_immobiliers
        case "141000": //immeubles
        case "111954": //obligations_liees_a_d_autres_instruments_financiers
        case "112954": //obligations_liees_a_d_autres_instruments_financiers_2
        case "112951": //obligations_liees_a_un_indice
        case "112950": //obligations_liees_a_un_panier_d_actions
        case "112955": //obligations_liees_a_un_panier_obligataire
        case "112100": //sukuk
            return new Bond();
        case "121160": // commodities_options
        case "121130": //currency_options
        case "121150": //future_options
        case "121140": //index_options
        case "121120": //interest_rate_options
        case "121165": //issued_commodities_options
        case "121135": //issued_currency_options
        case "121155": //issued_future_options
        case "121145": //issued_index_options
        case "121125": //issued_interest_rate_options
        case "121005": //issued_options
        case "121115": //issued_options_on_transferable_securities
        case "121195": //issued_swap_options
        case "121000": //options
        case "121110": //options_on_transferable_securities
        case "121190": //swap_options
        case "114000": //warrants_and_rights
        case "121185": //options_emises_sur_risque_de_credit
        case "121180": //options_sur_risque_de_credit
            return new Option();
        case "152319": //cfd
        case "131005": //current_accounts_at_bank
        case "999999": //current_accounts_at_bank_2
        case "122050": //guarantee_deposits_on_reverse_repurchase_agreements
        case "112007": //mezzanine_loans
        case "131013": //notification_deposits
        case "115500": //repurchase_agreements
        case "115000": //reverse_repurchase_agreements
        case "131010": //term_deposits
            return new Cash();
        case "112952": //equity_linked_certificates_1
        case "119952": //equity_linked_certificates
        case "111600": //investment_certificates
        case "111700": //participating_certificates
        case "111400": //participating_shares
        case "111000": //shares
        case "111900": //certificats_d_investissements
        case "111952": //equity_linked_zertifikate
            return new Equity();
        case "140": //foreign_exchange_contracts_linked_to_hedged_shares
        case "130": //forward_foreign_exchange_contracts
            return new FxForward();
        case "161500": //tracker_funds_opc
        case "161150": //tracker_funds_opcvm
        case "111500": //undertakings_for_collective_investment
        case "111951": //certificats_lies_a_un_indice
        case "111950": //certificats_lies_a_un_panier_de_titres
        case "112990": //fonds_commun_de_creance
        case "111550": //fonds_d_investissement_fermes
        case "161200": //fonds_d_investissement_opc
        case "161100": //fonds_d_investissement_opcvm
        case "111590": //fonds_immobiliers
        case "161700": //fonds_immobiliers_opc
        case "111580": //tracker_funds
        case "111650": //tracker_funds_fermes
            return new ShareClass();
            //case "111959": //finanzinnovationen
            //break;
            //case "112959": //finanzinnovationen_2
            //break;
    }

    return null;

    // subCategory1:
    //INVESTMENTS_IN_SECURITIES = 110000  ,
    //SHORT_POSITIONS_IN_SECURITIES = 115000  ,
    //REVERSE_REPURCHASE_AGREEMENTS = 117500  ,
    //REPURCHASE_AGREEMENTS = 117550  ,
    //FINANCIAL_INSTRUMENTS = 120000  ,
    //OPTIONS = 121000  ,
    //CASH_AT_BANKS = 131000  ,
    //BANK_LIABILITIES = 131500  ,
    //REAL_ESTATE_VALUES = 141000  ,
    //PRECIOUS_METALS = 150000  ,
    //OTHER_NET_ASSETS_LIABILITIES = 152000  ,
    //Liabilities = 200000
}
