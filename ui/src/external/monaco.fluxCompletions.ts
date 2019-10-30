import initialize from 'src/external/monaco'

initialize().then((monaco: any) => {
  monaco.languages.registerCompletionItemProvider('flux', {
    provideCompletionItems: () => {
      const suggestions = [
        {
          label: 'from',
          kind: monaco.languages.CompletionItemKind.Snippet,
          insertText: ['from(bucket: ${1})', '\t|>'].join('\n'),
          insertTextRules:
            monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          documentation: 'From-Statement',
        },
      ]
      return {suggestions: suggestions}
    },
  })
})
