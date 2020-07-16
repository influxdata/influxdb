import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'data',
  family: 'inputs',
  priority: 1,
  component: View,
  button: 'Bucket',
  initial: {
    bucketName: '',
    timeStart: '-1h',
    timeStop: 'now()',
  },
})
