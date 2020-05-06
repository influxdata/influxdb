import {register} from 'src/notebooks'
import View from './components'

register({
  type: 'visualization',
  component: View,
  button: 'Add Visualization',
  empty: {
      viewType: 'graph',
      viewProperties: {
      },
      displays: []
  },
})
