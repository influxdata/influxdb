import React from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

import {TEMP_VAR_DASHBOARD_TIME} from 'src/shared/constants'

const CustomTimeIndicator = ({queries}) => {
  const q = queries.find(({query}) => !query.includes(TEMP_VAR_DASHBOARD_TIME))
  const customLower = _.get(q, ['queryConfig', 'range', 'lower'], null)
  const customUpper = _.get(q, ['queryConfig', 'range', 'upper'], null)

  if (!customLower) {
    return null
  }

  const customTimeRange = customUpper
    ? `${customLower} AND ${customUpper}`
    : customLower

  return <span className="custom-indicator">{customTimeRange}</span>
}

const {arrayOf, shape} = PropTypes

CustomTimeIndicator.propTypes = {
  queries: arrayOf(shape()),
}

export default CustomTimeIndicator
