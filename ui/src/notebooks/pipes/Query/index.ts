import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'query',
  component: View,
  button: 'Custom Script',
  initial: {
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
