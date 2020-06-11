import {register} from 'src/notebooks'
import View from './view'
import './style.scss'

register({
  type: 'dance',
  component: View,
  button: 'Dance',
  initial: {},
})
