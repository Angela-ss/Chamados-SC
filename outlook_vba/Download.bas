Attribute VB_Name = "Download"
Sub downloadAnexo(MItem As Outlook.MailItem)
    
    On Error Resume Next
    
    Dim OutAnexo As Outlook.Attachment
    Dim caminho_completo As String
    
    caminho_completo = "C:\Users\agsilva11\OneDrive - Stefanini\Documents\Particular\Atualizar_TBLCHAMADOS"
    
    nome_arquivo = MItem.Subject
    nome_arquivo = nome_arquivo & " " & Format(Now, "dd-mm-yyyy")
    
    For Each OutAnexo In MItem.Attachments
        OutAnexo.SaveAsFile caminho_completo & "\" & nome_arquivo & ".xlsx"
    Next

End Sub
