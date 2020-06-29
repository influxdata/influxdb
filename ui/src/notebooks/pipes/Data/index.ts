import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'data',
  priority: 1,
  featureFlag: 'notebook-panel--data-source',
  component: View,
  button: 'Data Source',
  initial: {
    bucketName: '',
    timeStart: '-1h',
    timeStop: 'now()',
  },
})
