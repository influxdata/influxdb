import {register} from 'src/notebooks'
import View from './components'
import './style.scss'

register({
  type: 'query',
  component: View,
  button: 'Add Custom Script',
  empty: {
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
