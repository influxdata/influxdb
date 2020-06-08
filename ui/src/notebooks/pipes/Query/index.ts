import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

export type RawDataSize = 'small' | 'medium' | 'large'

register({
  type: 'query',
  component: View,
  button: 'Custom Script',
  initial: {
    rawDataSize: 'small',
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
