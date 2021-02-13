
var parentSection = NewSection("NAV Estimation","NavEstimation");

var portfolio = Referential.Portfolios.First(i => i.Id == Parameters.SubFundId);
if (!portfolio.Nav.HasValue) 
    throw new Exception($"Current NAV of {portfolio.Name} not available");
if (!portfolio.Tna.HasValue) 
    throw new Exception($"Current TNA of {portfolio.Name} not available");

var securities = Referential.Securities.ToDictionary(i => i.Id, i => i);

//Computation of the new position market values
//!! Apply transactions here
var posVal = Referential.LastPositions.Where(i => i.PortfolioId == portfolio.Id)
        .Select(pos => new {
                SecurityId = pos.SecurityId, 
                NewQuantity = pos.Quantity,
                PriceDate = (securities[pos.SecurityId].PriceDate != null)? securities[pos.SecurityId].PriceDate.Value : DateTime.MinValue,
                CurrentMarketValueInPortfolioCcy = pos.MarketValueInPortfolioCcy,
                EstimatedMarketValueInPortfolioCcy = 
                        securities[pos.SecurityId].Type == "Cash"? pos.MarketValueInPortfolioCcy
                        :(securities[pos.SecurityId].Price == null)? pos.MarketValueInPortfolioCcy
                        :(securities[pos.SecurityId].Price.Value * pos.Quantity) / getFxRate(portfolio.CcyIso,securities[pos.SecurityId].CcyIso)
        })
        .ToList();

//Computation, for each share-classes of the esitmated NAV, TNA and NBS
double totalTnaEstimation = posVal.Any(i => !i.EstimatedMarketValueInPortfolioCcy.HasValue )? double.NaN
        : posVal.Sum(i => i.EstimatedMarketValueInPortfolioCcy.Value);

DateTime navEstimationDate = posVal.Max(i => i.PriceDate);

double sumCurrentMvs = Referential.LastPositions.Where(i => i.PortfolioId == portfolio.Id)
        .Sum(i=>i.MarketValueInPortfolioCcy.Value); // from Universal

//-------- SHARE CLASSES NAV --------
foreach (var sc in Referential.InternalShareClasses.Where(i=>i.PortfolioId == portfolio.Id).OrderByDescending(i => i.Tna.Value))
{       
        double newSubsriptionInShares = 0.0; //!! To be provided by BBH
        double newRedemptionInShares = 0.0; //!! To be provided by BBH
        double newNbShares = sc.Nbs.Value + newSubsriptionInShares - newRedemptionInShares;

        double newShareClassTna = sc.Tna.Value * (totalTnaEstimation / sumCurrentMvs); 

        double mgmtFeeProRata = (sc.ManagementFee.HasValue)? 
                        sc.ManagementFee.Value * ((navEstimationDate - portfolio.NavDate.Value).TotalDays / 365.0)
                        : throw new Exception("Management fee not provided for share class: "+sc.Name);
        double mgmtFeeAmount = newShareClassTna * mgmtFeeProRata;
        
        newShareClassTna = newShareClassTna - mgmtFeeAmount;  
        double newNav = newShareClassTna  / newNbShares;
        
        double benchmarkPerformance = double.NaN;
        var schvs = Referential.ShareClassHistValues.Where(i => i.ShareClassId == sc.Id).OrderByDescending(i=>i.Date).Take(2).ToList();
        if ((schvs.Count == 2) && schvs.First().Date == navEstimationDate)
                benchmarkPerformance = (schvs.First().BenchmarkAdjustedPrice.Value / schvs.Last().BenchmarkAdjustedPrice.Value) - 1;

        var table1 = new List<MacroResultRow>();
        table1.Add(NewListRow($"Estimated NAV",$"EstimatedNAV-{sc.Id}" , new[] {
                NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
                NewValue("Value").SetNumber(newNav),
                NewValue("% Change","pChange").SetPercentage((newNav/sc.Nav.Value)-1)
                        .SetDirection(GetDirection(newNav, sc.Nav.Value))
                        .SetSentiment(GetSentiment(newNav, sc.Nav.Value)),
                }));
        
        table1.Add(NewListRow($"Benchmark % Change",$"BenchmarkPChange" , new[] {
                NewValue("As of Date","AsOfDate").SetDate(schvs.First().Date),
                NewValue("Value").SetText("-"),
                NewValue("% Change","pChange").SetPercentage(benchmarkPerformance)
                        .SetDirection(GetDirection(schvs.First().BenchmarkAdjustedPrice, schvs.Last().BenchmarkAdjustedPrice))
                        .SetSentiment(GetSentiment(schvs.First().BenchmarkAdjustedPrice, schvs.Last().BenchmarkAdjustedPrice)),
                }));

        table1.Add(NewListRow($"Current NAV (from FundAdmin)",$"CurrentNAV-{sc.Id}" , new[] {
                NewValue("As of Date","AsOfDate").SetDate(sc.NavDate.Value),
                NewValue("Value").SetNumber(sc.Nav.Value),
                NewValue("% Change","pChange").SetText("")
                }));

        table1.Add(NewListRow($"Estimated AUM",$"EstimatedAUM-{sc.Id}", new[] {
                NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
                NewValue("Value").SetNumber(newShareClassTna),
                NewValue("% Change","pChange").SetPercentage((newShareClassTna/sc.Tna.Value)-1)
                        .SetDirection(GetDirection(newShareClassTna, sc.Tna.Value))
                        .SetSentiment(GetSentiment(newShareClassTna, sc.Tna.Value)),
                }));      

        table1.Add(NewListRow($"Current AUM  (from FundAdmin)",$"CurrentAUM-{sc.Id}", new[] {
                NewValue("As of Date","AsOfDate").SetDate(sc.NavDate),
                NewValue("Value").SetNumber(sc.Tna.Value),
                NewValue("% Change","pChange").SetText("")
                }));
        table1.Add(NewListRow("Management Fees","MgmtFee" ,new[] {
                NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
                NewValue("Value").SetNumber(mgmtFeeAmount),
                NewValue("% Change","pChange").SetText("")
                }));
        table1.Add(NewListRow("Total Subscription (waiting BBH files)", new[] {
                NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
                NewValue("Value").SetText("N/A"),
                NewValue("% Change","pChange").SetText("")
                }));

        table1.Add(NewListRow("Total Redemption (waiting BBH files)", new[] {
                NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
                NewValue("Value").SetText("N/A"),
                NewValue("% Change","pChange").SetText("")
                }));

        parentSection.AddList($"{sc.Name} - Estimated NAV",$"{sc.Name}_NavEstimation - {portfolio.InternalCode}", table1);
}

//-------- SUB FUND TNA --------
var tableTna = new List<MacroResultRow>();
tableTna.Add(NewListRow("Estimated AUM", $"Estimated AUM-{portfolio.Id}", new[] {
        NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
        NewValue("TNA","totalTna").SetNumber(totalTnaEstimation),
        NewValue("% Change","pChange").SetPercentage((totalTnaEstimation/portfolio.Tna.Value)-1)
                        .SetDirection(GetDirection(totalTnaEstimation, portfolio.Tna.Value))
                        .SetSentiment(GetSentiment(totalTnaEstimation, portfolio.Tna.Value)),
        }));

tableTna.Add(NewListRow("Current AUM (from FundAdmin)", new[] {
        NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
        NewValue("TNA","totalTna").SetNumber(portfolio.Tna.Value),
        NewValue("% Change","pChange").SetText("")
        }));

tableTna.Add(NewListRow("Sum of Position Current Values", new[] {
        NewValue("As of Date","AsOfDate").SetDate(navEstimationDate),
        NewValue("TNA","totalTna").SetNumber(sumCurrentMvs),
        NewValue("% Change","pChange").SetText("")
        }));


 parentSection.AddList($"Sub-Fund - Total Net Asset",$"SubFundTNA", tableTna);

//-------- POSITIONS --------
var positionRows = posVal.OrderByDescending(i => i.EstimatedMarketValueInPortfolioCcy)
        .Select(newPos =>
            NewListRow($"{securities[newPos.SecurityId].Name}",$"{newPos.SecurityId}", new[] {                
                securities[newPos.SecurityId].PriceDate.HasValue? 
                    NewValue("Price Date","Price Date").SetDate(securities[newPos.SecurityId].PriceDate.Value)
                    : NewValue("Price Date","Price Date").SetText("-"),
                securities[newPos.SecurityId].Price.HasValue? 
                    NewValue("Price").SetNumber(securities[newPos.SecurityId].Price.Value)
                    : NewValue("Price").SetText("-"),
                NewValue("Ccy").SetText(securities[newPos.SecurityId].CcyIso),
                getFxRate(portfolio.CcyIso,securities[newPos.SecurityId].CcyIso).HasValue?
                        NewValue($"FxRate","FxRate").SetNumber(getFxRate(portfolio.CcyIso,securities[newPos.SecurityId].CcyIso).Value)
                        : NewValue($"FxRate","FxRate").SetText("N/A"),
                NewValue("Q").SetNumber(newPos.NewQuantity),
                NewValue($"Current FA Value ({portfolio.NavDate.Value.ToString("dd/MM")})","CurrentValue")
                                .SetNumber(newPos.CurrentMarketValueInPortfolioCcy),
                newPos.EstimatedMarketValueInPortfolioCcy.HasValue?
                        NewValue($"Estimated Value ({navEstimationDate.ToString("dd/MM")})","EstimatedValue")
                                .SetNumber(newPos.EstimatedMarketValueInPortfolioCcy.Value)
                        : NewValue($"Estimated Value ({navEstimationDate.ToString("dd/MM")})","EstimatedValue")
                                .SetText("N/A"),
                newPos.EstimatedMarketValueInPortfolioCcy.HasValue?
                        NewValue($"% Change","pChange")
                                .SetPercentage((newPos.EstimatedMarketValueInPortfolioCcy.Value/newPos.CurrentMarketValueInPortfolioCcy)-1)
                                .SetSentiment(GetSentiment(newPos.EstimatedMarketValueInPortfolioCcy, newPos.CurrentMarketValueInPortfolioCcy))
                                .SetDirection(GetDirection(newPos.EstimatedMarketValueInPortfolioCcy, newPos.CurrentMarketValueInPortfolioCcy))

                        : NewValue($"% Change","pChange").SetText("N/A"),
        }))
        .ToList();
parentSection.AddList("Positions","PositionValuation", positionRows);

#region Helpers

double? getFxRate(string portCcy,string ccyIso)
{
        if (portCcy == ccyIso)
                return 1;
        else if (ccyIso == "GBx")
        {
                if (!Referential.PricingPolicyLastFxRates.Any(i => i.CcyToIso == "GBP"))
                        return null;   
                else
                        return Referential.PricingPolicyLastFxRates.First(i => i.CcyToIso == "GBP").RateFromReferenceCurrency *100;
        }
        else if (!Referential.PricingPolicyLastFxRates.Any(i => i.CcyToIso == ccyIso))
                return null;
        return Referential.PricingPolicyLastFxRates.First(i => i.CcyToIso == ccyIso).RateFromReferenceCurrency;
}
MacroResultSentiment GetSentiment(double? estimated, double? current)
{   
        if (!estimated.HasValue || !current.HasValue)
                return MacroResultSentiment.Neutral;

        var perf = (estimated.Value / current.Value)-1;
        if (Math.Abs(perf) < 0.001)
        return MacroResultSentiment.Neutral;

        if (perf < 0)
        return MacroResultSentiment.Negative;
        return MacroResultSentiment.Positive;
}

MacroResultDirection GetDirection(double? estimated, double? current)
{
        if (!estimated.HasValue || !current.HasValue)
                return MacroResultDirection.Still;
        return GetDirection(estimated.Value, current.Value);
}
MacroResultDirection GetDirection(double estimatedNav, double currentNav)
{
    var perf = (estimatedNav/currentNav)-1; 
    if (Math.Abs(perf) < 0.001)
        return MacroResultDirection.Still;
    if (perf < 0)
    {
        if (perf < -0.015) 
                return MacroResultDirection.LargeDown;
        return MacroResultDirection.Down;
    }
    else
    {
        if (perf > 0.015) 
                return MacroResultDirection.LargeUp;
        return MacroResultDirection.Up;
    }
}
#endregion Helpers

parentSection