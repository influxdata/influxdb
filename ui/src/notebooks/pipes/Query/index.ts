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
        text: '// Tip: Use the __PREVIOUS_RESULT__ variable to link your queries\n\n',
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
