var secPosFileDefinition = FlatFileDefinition.Create(i => new
{
    Date_Position = i.ToDateColumn("Date_Position","dd/MM/yyyy"), //26/11/2020
    Matricule = i.ToColumn<string>("Matricule"), //9237
    Intitule = i.ToColumn<string>("Intitule"), //IBP VOLVO BELGIUM OFP     
    Numero = i.ToColumn<string>("Numero"), //676-0923701-33
    Nature = i.ToColumn<string>("Nature"), //Ordinaire
    Qualite = i.ToColumn<string>("Qualite"), //Org. Financement Pensions (002)
    Clef_Alpha_Gest = i.ToColumn<string>("Clef_Alpha_Gest"), //SHELTER*IM*TG
    Nom_Gest = i.ToColumn<string>("Nom_Gest"), //SHELTER IM S.A.
    ISIN = i.ToColumn<string>("ISIN"), //BE0003739530
    NOVALSYS = i.ToColumn<string>("NOVALSYS"), //38918
    Bloomberg = i.ToColumn<string>("Bloomberg"), //Ticker	UCB BB
    Intitule_Valeur = i.ToColumn<string>("Intitule_Valeur"), //UCB
    Categorie_Valeur = i.ToColumn<string>("Categorie_Valeur"), //ACTION
    Variante_Valeur = i.ToColumn<string>("Variante_Valeur"), //Action
    Place = i.ToColumn<string>("Place"), //Bruxelles
    Quantite = i.ToColumn<string>("Quantite"), //3856
    Dernier_Cours = i.ToColumn<string>("Dernier_Cours"), //90.84
    Date_Cours = i.ToColumn<string>("Date_Cours"), //26/11/2020
    Valorisation_Devise = i.ToColumn<string>("Valorisation_Devise"), //350279.04
    Devise = i.ToColumn<string>("Devise"), //EUR
    Valorisation_Eur = i.ToColumn<string>("Valorisation_Eur"), //350279.04
    FileName = i.ToSourceName(),
}).IsColumnSeparated(',');

var cashPosFileDefinition = FlatFileDefinition.Create(i => new
{
    Date_Solde = i.ToDateColumn("Date_Solde","dd/MM/yyyy"), //26/11/2020
    Matricule = i.ToColumn<string>("Matricule"), //9237
    Intitule = i.ToColumn<string>("Intitule"),   //IBP VOLVO BELGIUM OFP     
    Numero = i.ToColumn<string>("Numero"),   //676-0923701-33
    Nature = i.ToColumn<string>("Nature"),   //Ordinaire
    Qualite = i.ToColumn<string>("Qualite"), //Org. Financement Pensions (002)
    Clef_Alpha_Gest = i.ToColumn<string>("Clef_Alpha_Gest"), //SHELTER*IM*TG
    Nom_Gest = i.ToColumn<string>("Nom_Gest"),   //SHELTER IM S.A.
    Solde_Cash_Devise = i.ToColumn<string>("Solde_Cash_Devise"), //123.63
    Devise = i.ToColumn<string>("Devise"),   //CHF
    Solde_Cash_Eur = i.ToColumn<string>("Solde_Cash_Eur"),   //114.33
    FileName = i.ToSourceName(),
}).IsColumnSeparated(',');

var secPosFileStream = FileStream
    .Where($"{TaskName} Only Security positions file",i => i.Name.ToLower().Contains("posiall"))
    .CrossApplyTextFile($"{TaskName}: parse position file", secPosFileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");