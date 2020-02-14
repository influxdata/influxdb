// Constants
import {FLUXLANGID} from 'src/external/constants'

export const tokenizeFlux = monaco => {
  monaco.languages.register({id: FLUXLANGID})

  monaco.languages.setMonarchTokensProvider(FLUXLANGID, {
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
