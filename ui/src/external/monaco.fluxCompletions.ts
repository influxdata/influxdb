// Types
import {MonacoType} from 'src/types'

export const addSnippets = (monaco: MonacoType) => {
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
      ] as any[]
      return {suggestions: suggestions}
    },
  })
}
