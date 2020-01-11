import {Threshold} from 'src/types'
import {isNaN} from 'lodash'

export const checkThresholdsValid = (thresholds: Threshold[]) => {
  thresholds.forEach(t => {
    if (t.type === 'greater' && isNaN(t.value)) {
      throw new Error('Threshold must have defined value')
    }
    if (t.type === 'lesser' && isNaN(t.value)) {
      throw new Error('Threshold must have defined value')
    }
    if (t.type === 'range' && (isNaN(t.min) || isNaN(t.min))) {
      throw new Error('Threshold must have defined values')
    }
  })
}
