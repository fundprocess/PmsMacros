var sum = Referential.Positions
	.Where(position=>position.PortfolioId == Parameters.SubFundId && position.Weight>0.05)
	.Sum(i=>i.Weight);

NewValue("Sum 05/04")
	.SetNumber(sum)
	.SetValidity(sum.Choose(MacroResultValidity.Valid, 0.4, MacroResultValidity.Invalid))