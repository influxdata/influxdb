import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

export type ResultsVisibility = 'visible' | 'hidden'

register({
  type: 'query',
  component: View,
  button: 'Custom Script',
  initial: {
    resultsVisibility: 'visible',
    resultsPanelHeight: undefined,
    activeQuery: 0,
    queries: [
      {
        text: '',
        editMode: 'advanced',
        builderConfig: {
          buckets: [],
          tags: [],
          functions: [],
        },
      },
    ],
  },
})
