import initialize from 'src/external/monaco'

initialize().then((monaco: any) => {
  monaco.languages.register({id: 'Flux'})

  monaco.languages.setMonarchTokensProvider('Flux', {
    keywords: ['from', 'range', 'filter', 'to'],
    tokenizer: {
      root: [
        [
          /[a-z_$][\w$]*/,
          {cases: {'@keywords': 'keyword', '@default': 'identifier'}},
        ],
      ],
    },
  })

  monaco.editor.defineTheme('FluxTheme', {
    base: 'vs',
    inherit: false,
    rules: [{token: 'keyword', foreground: 'ff00ff', fontStyle: 'bold'}],
  })

  monaco.languages.registerCompletionItemProvider('Flux', {
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
