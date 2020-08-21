import {register} from 'src/notebooks'
import View from './view'
import './style.scss'
import {FUNCTIONS} from 'src/timeMachine/constants/queryBuilder'

register({
  type: 'data',
  family: 'inputs',
  priority: 1,
  component: View,
  button: 'Bucket',
  initial: {
    bucketName: '',
    field: '',
    measurement: '',
    tags: {},
    aggregateFunction: FUNCTIONS[0],
  },
})
