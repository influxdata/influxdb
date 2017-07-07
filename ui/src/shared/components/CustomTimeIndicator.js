import React, {PropTypes} from 'react'
import _ from 'lodash'

const CustomTimeIndicator = ({queries}) => {
  if (!queries || !queries.length) {
    return null
  }

  const q = queries.find(({query}) => !query.includes(':dashboardTime:'))
  const customLower = _.get(q, ['queryConfig', 'range', 'lower'], null)
  const customUpper = _.get(q, ['queryConfig', 'range', 'upper'], null)

  if (!customLower) {
    return null
  }

  const customTimeRange = customUpper
    ? `${customLower} AND ${customUpper}`
    : customLower

  return <span className="dash-graph--custom-time">{customTimeRange}</span>
}

const {arrayOf, shape} = PropTypes

CustomTimeIndicator.propTypes = {
  queries: arrayOf(shape()),
}

export default CustomTimeIndicator
