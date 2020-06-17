import {register} from 'src/notebooks'

import View from './view'
import './style.scss'

register({
  type: 'spotify',
  priority: -1,
  featureFlag: 'notebook-panel--spotify',
  button: 'Music',
  component: View,
  initial: {
    uri: 'spotify:track:55A8N3HXzIecctUSvru3Ch',
  },
})
