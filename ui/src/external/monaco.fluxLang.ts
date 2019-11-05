export const tokenizeFlux = monaco => {
  monaco.languages.register({id: 'flux'})

  monaco.languages.setMonarchTokensProvider('flux', {
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
}
