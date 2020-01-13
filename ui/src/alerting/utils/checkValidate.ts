import {Threshold} from 'src/types'

export const checkThresholdsValid = (thresholds: Threshold[]) => {
  thresholds.forEach(t => {
    if (t.type === 'greater' && isNaN(t.value)) {
      throw new Error('Threshold must have defined value')
    }
    if (t.type === 'lesser' && isNaN(t.value)) {
      throw new Error('Threshold must have defined value')
    }
    if (t.type === 'range' && (isNaN(t.min) || isNaN(t.min))) {
      throw new Error('Threshold must have defined min and max values')
    }
  })
}
