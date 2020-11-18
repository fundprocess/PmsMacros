var lastCompositionStream=ProcessContextStream.EfCoreSelect("Get Compositions", i => i
    .Set<PortfolioComposition>()
    .Include(inc=>inc.Positions)
    .Where(p => p.Portfolio.InternalCode == "6373008")
    )
    .Aggregate("get last compo per portfolio",
        i=>i.PortfolioId,
        i=>(PortfolioComposition)null,
        (a,v)=>{
            if(a == null) return v;
            if(a.Date < v.Date) return v;
            return a;
        });

var transactionStream = ProcessContextStream.EfCoreSelect("Get Transactions", i => i
    .Set<SecurityTransaction>()
    .Where(p => p.Portfolio.InternalCode == "6373008")
    ).
    Aggregate("group transaction by portfolio", 
        i=>i.PortfolioId, 
        i=>new List<SecurityTransaction>(),
        (a,v)=> {
            a.Add(v);
            return a;
        });

var portfolioStream = ProcessContextStream.EfCoreSelect("Get Portfolios", i => i
    .Set<DiscretionaryPortfolio>()
    .Where(p => p.InternalCode == "6373008")
    )
    .Lookup("Get related last compo", 
        lastCompositionStream, 
        l=>l.Id, 
        r=>r.Key, 
        (l, r) => new {
            PortfolioId=l.Id,
            Portfolio=l,
            Composition=r?.Aggregation
        })
    .Lookup("Get relation transactions",
        transactionStream,
        l=>l.PortfolioId,
        r=>r.Key,
        (l,r)=>new {
            Portfolio=l.Portfolio,
            Composition=l.Composition,
            Transactions=r?.Aggregation
        })
    .CrossApplyEnumerable("Create new compositions", i=> GetCompositions(i.Portfolio, i.Composition, i.Transactions))
    .EfCoreDelete("Delete current positions", o=>o
        .Set<Position>()
        .Where((composition, position)=>position.PortfolioComposition.Date == composition.Date && position.PortfolioComposition.PortfolioId == composition.PortfolioId ))
    .EfCoreSave("Save compos", o=>o
        .SeekOn(i=>new {i.PortfolioId, i.Date})
        .WithMode(SaveMode.EntityFrameworkCore));


List<PortfolioComposition> GetCompositions(DiscretionaryPortfolio portfolio, PortfolioComposition lastComposition, List<SecurityTransaction> securityTransactions){
    if(lastComposition==null && securityTransactions==null)
        return new List<PortfolioComposition>();
    if(lastComposition==null){
        lastComposition=new PortfolioComposition{
            Positions=new List<Position>(),
            Date=securityTransactions.Select(i=>i.NavDate).Min()
        };
    }
    if(securityTransactions==null)
        securityTransactions=new List<SecurityTransaction>();
    var dates = GetNavDates(
        lastComposition.Date, 
        securityTransactions.Select(i=>i.NavDate).DefaultIfEmpty(DateTime.Today).Max());
    var compositions=new List<PortfolioComposition>();
    foreach (var navDate in dates.OrderBy(i=>i).ToList())
    {
        var dayTrades = securityTransactions.Where(i => i.NavDate == navDate).ToList();
        var newPositions = GetUpdatedPositions(lastComposition.Positions, dayTrades);

        //VALUATION
        foreach (var newPosition in newPositions)
        {
            // var positionValuation = ValuationProvider.GetValuationFromQuantity(newPosition.SecurityId,newPosition.Value,portfolio.Key);
            
            // newPosition.MarketValueInSecurityCcy = positionValuation.MarketValueInSecurityCcy;          
            // newPosition.ValuationPrice = positionValuation.ValuationPrice;

            var currentPosition = lastComposition.Positions.FirstOrDefault(i=>i.SecurityId == newPosition.SecurityId);
            var tradesForSecurity=dayTrades.Where(i => i.SecurityId == newPosition.SecurityId).ToList();
            if (!dayTrades.Any(i=>i.SecurityId == newPosition.SecurityId)) //No trade on this security today
                newPosition.CostPrice= currentPosition.CostPrice;

            else if (currentPosition==null) //New purchase
                newPosition.CostPrice = tradesForSecurity.Sum(i=> i.Quantity * i.PriceInSecurityCcy)
                                    / tradesForSecurity.Sum(i=> i.Quantity);

            else if (currentPosition !=null && newPosition.Value > currentPosition.Value) //Purchase: Position increase
            {
                var num = (currentPosition.Value * currentPosition.CostPrice ) 
                        + tradesForSecurity.Select(i=> i.Quantity * i.PriceInSecurityCcy).Sum();
                var den = currentPosition.Value + tradesForSecurity.Sum(i=> i.Quantity);
                newPosition.CostPrice = num / den;
            }
            else if (currentPosition !=null && newPosition.Value < currentPosition.Value) //Sale
                newPosition.CostPrice =  GetCostFIFO(tradesForSecurity); // Compute FIFO!                         
            // newPos.BookCostInPortfolioCcy
            // newPos.BookCostInSecurityCcy
        }
        lastComposition=new PortfolioComposition { PortfolioId = portfolio.Id, Date = navDate, Positions = newPositions };
        compositions.Add(lastComposition);
    }
    return compositions;
}

List<DateTime> GetNavDates(DateTime minDate, DateTime maxDate)
{
    var holidayDates = new List<DateTime>(); //...TBC
    
    var calculationDates = new List<DateTime>();
    var currentDate = maxDate;
    while (currentDate >= minDate)
    {
        calculationDates.Add(currentDate);
        currentDate = currentDate.AddDays(-1);
    }
    return calculationDates.Except(holidayDates).OrderBy(i => i).ToList();
}

List<Position> GetUpdatedPositions(List<Position> currentPositions, List<SecurityTransaction> trades)
{
    var newPositions = currentPositions.Select(i => new Position() { SecurityId = i.SecurityId, Value = i.Value, MarketValueInPortfolioCcy=0,Weight=0 }).ToList();

    if (trades == null || !trades.Any())
        return newPositions;

    var applicableTypes = new List<string>(){"SUBSCRIPTION","SALE","PURCHASE","FUND SUBSCRIPTION"};

    trades = trades.Where(i=>applicableTypes.Any(tradeType=>tradeType==i.Description)).ToList();
    foreach (var trade in trades)
    {
        Position existingPosition = currentPositions.Where(p => p.SecurityId == trade.SecurityId).FirstOrDefault();

        if (existingPosition == null && trade.OperationType == OperationType.Buy) //New buy
            newPositions.Add(new Position(){ SecurityId = trade.SecurityId, Value = Math.Abs(trade.Quantity)} );

        else if (existingPosition != null && trade.OperationType == OperationType.Buy) //Increase
            existingPosition.Value = existingPosition.Value + Math.Abs(trade.Quantity);

        else if (existingPosition != null && (trade.OperationType == OperationType.Sale)) //Decrease
            existingPosition.Value = existingPosition.Value - Math.Abs(trade.Quantity);
        else
            throw new System.Exception($"No sale allowed on not existing position: {trade.SecurityId} - Q={trade.Quantity}");
    }
    return newPositions.Where(i=>i.Value!=0).ToList(); //We remove total sales
}



double GetCostFIFO(List<SecurityTransaction> trades)
{
    return -1;
}