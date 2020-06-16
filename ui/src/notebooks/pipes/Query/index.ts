import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'query',
  priority: 1,
  component: View,
  button: 'Flux Script',
  initial: {
    panelVisibility: 'visible',
    panelHeight: 200,
    activeQuery: 0,
    queries: [
      {
        text: '// Write Flux script here',
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
