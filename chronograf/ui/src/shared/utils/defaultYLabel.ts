import _ from 'lodash'
import {fieldWalk} from 'src/shared/reducers/helpers/fields'

export const buildDefaultYLabel = queryConfig => {
  const {measurement} = queryConfig
  const fields = _.get(queryConfig, ['fields', '0'], [])
  const isEmpty = !measurement && !fields.length

  if (isEmpty) {
    return ''
  }

  const walkZerothArgs = f => {
    if (f.type === 'field') {
      return f.value
    }

    return `${f.value}${_.get(f, ['0', 'args', 'value'], '')}`
  }

  const values = fieldWalk([fields], walkZerothArgs)

  return `${measurement}.${values.join('_')}`
}
