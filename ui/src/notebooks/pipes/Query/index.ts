import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'query',
  family: 'custom',
  priority: 1,
  component: View,
  button: 'Flux Script',
  initial: {
    panelVisibility: 'visible',
    panelHeight: 200,
    activeQuery: 0,
    queries: [
      {
        text:
          '// Write Flux script here\n// use __PREVIOUS_RESULT__ to continue building from the previous cell',
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
