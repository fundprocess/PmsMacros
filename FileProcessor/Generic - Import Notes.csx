//Notices:
//  - Policy: overwrite
//  - remove the "carriage returns" in input file
//to remove carriage returns in Excel: Replace All (ctrl-h)  replace "ctrl-j" by ""

var fileDefinition = FlatFileDefinition.Create(i => new
{
    EntityCode = i.ToColumn("EntityCode"),
    EntityName = i.ToColumn("EntityName"),
    EntityType = i.ToColumn("EntityType"),
    Date = i.ToDateColumn("Date","yyyy-MM-dd"),
    CategoryCode = i.ToColumn("CategoryCode"),
    CategoryName = i.ToColumn("CategoryName"),
    Author = i.ToColumn("Author"),
    LastModificationDate = i.ToDateColumn("LastModificationDate","yyyy-MM-dd"),
    HtmlTextEn = i.ToColumn("HtmlTextEn"),
    HtmlTextFr = i.ToColumn("HtmlTextFr"),
    HtmlTextNl = i.ToColumn("HtmlTextNl"),
}).IsColumnSeparated(',').WithEncoding(System.Text.Encoding.GetEncoding(1252));

var notesFileStream = FileStream
    .CrossApplyTextFile($"{TaskName}: parse file", fileDefinition)
    .SetForCorrelation($"{TaskName}: Set correlation key");

var noteCategoriesStream = notesFileStream
    .Distinct($"{TaskName}: Distinct category", i=>i.CategoryCode)
    .Select($"{TaskName}: Create note category", i=> new NoteCategory() { Code = i.CategoryCode, Name = i.CategoryName })
    .EfCoreSave($"{TaskName}: Save note category", i => i.SeekOn(i=>i.Code));

var personsDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get persons stream from db", (ctx, j) => ctx.Set<Person>());

var notesStream = notesFileStream
    .CorrelateToSingle($"{TaskName} Get related category",noteCategoriesStream,(l,r) => new {FileRow = l, NoteCategory = r})
    .Lookup($"{TaskName} Get note author", personsDbStream, i=> i.FileRow.Author, i=>i.InternalCode,
         (l,r) => new {FileRow = l.FileRow, NoteCategory = l.NoteCategory, Author = r})
    .Select($"{TaskName}: Create note", i=> 
        new Note() { 
            AuthorId = (i.Author != null)? i.Author.Id : throw new Exception("Author not found in CRM: " + i.FileRow.Author),
            CategoryId = (i.NoteCategory != null)? i.NoteCategory.Id : throw new Exception("Note category not found" + i.FileRow.CategoryCode),
            Date = i.FileRow.Date,
            LastModificationDate = i.FileRow.LastModificationDate,
            Text = new MultiCultureString { ["en"] = i.FileRow.HtmlTextEn,["fr"] = i.FileRow.HtmlTextFr,["nl"] = i.FileRow.HtmlTextNl }
    })
    .EfCoreSave($"{TaskName}: Save note",i => i.SeekOn(i=>new {i.CategoryId, i.Date,i.LastModificationDate }));

var portfolioNotesFileStream = notesFileStream
    .Where($"{TaskName}: where is portfolio note", i=> !string.IsNullOrEmpty(i.EntityType) && i.EntityType.ToLower() == "portfolio");

var portfolioNotesStream = portfolioNotesFileStream
    .CorrelateToSingle($"{TaskName}: Get related note",notesStream,
        (l,r) => new {FileRow = l, Note = r})
    .LookupPortfolio($"{TaskName}: Get related portfolio", i=>i.FileRow.EntityCode,
        (l,r) => new {FileRow = l.FileRow, Note = l.Note, Portfolio = r })
    .Where($"{TaskName}: Where portfolio is not null", i=> i.Portfolio !=null)
    .Select($"{TaskName}: Create portfolio notes", i => new PortfolioNote{
        NoteId = i.Note !=null? i.Note.Id : throw new Exception("Note not found"),
        PortfolioId = i.Portfolio.Id,
    })
    .EfCoreSave($"{TaskName}: Save portfolio notes",  i => i.SeekOn(i=>new {i.NoteId,i.PortfolioId}));

var sicavNotesFileStream = notesFileStream.Where($"{TaskName} where is SICAV note", i=> 
   !string.IsNullOrEmpty(i.EntityType) && i.EntityType.ToLower() == "sicav");

var sicavDbStream = ProcessContextStream.EfCoreSelect($"{TaskName}: get Sicav stream from db", (ctx, j) => ctx.Set<Sicav>());

var sicavNotesStream = sicavNotesFileStream
    .CorrelateToSingle($"{TaskName}: Get related sicav note",notesStream,
        (l,r) => new {FileRow = l, Note = r})
    .Lookup($"{TaskName}: Get related sicav", sicavDbStream, i=>i.FileRow.EntityCode, i=>i.InternalCode,
        (l,r) => new {FileRow = l.FileRow, Note = l.Note, Sicav = r })
    .Where($"{TaskName}: Where sicav is not null", i=> i.Sicav !=null)
    .Select($"{TaskName}: Create sicav notes", i => new SicavNote{
        NoteId = i.Note !=null? i.Note.Id : throw new Exception("Note not found"),
        SicavId = i.Sicav.Id,
    })
    .EfCoreSave($"{TaskName}: Save Sicav notes",  i => i.SeekOn(i=>new {i.NoteId,i.SicavId}));

return FileStream.WaitWhenDone($"{TaskName}: wait till everything is saved",noteCategoriesStream,notesStream,
    portfolioNotesStream,sicavNotesStream);