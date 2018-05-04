import React from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import _ from 'lodash'

import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'
import {timeSeriesToTableGraph} from 'src/utils/timeSeriesTransformers'
import {dataToCSV} from 'src/shared/parsing/resultsToCSV'
import download from 'src/external/download.js'
import {TEMPLATES} from 'src/shared/constants'

const getDataForCSV = (query, errorThrown) => async () => {
  try {
    const response = await fetchTimeSeriesAsync({
      source: query.host,
      query,
      tempVars: TEMPLATES,
    })
    const {data} = timeSeriesToTableGraph([{response}])

    download(dataToCSV(data), `${''}.csv`, 'text/plain')
  } catch (error) {
    errorThrown(error, 'Unable to download .csv file')
    console.error(error)
  }
}

const VisHeader = ({views, view, onToggleView, query, errorThrown}) => (
  <div className="graph-heading">
    {views.length ? (
      <ul className="nav nav-tablist nav-tablist-sm">
        {views.map(v => (
          <li
            key={v}
            onClick={onToggleView(v)}
            className={classnames({active: view === v})}
            data-test={`data-${v}`}
          >
            {_.upperFirst(v)}
          </li>
        ))}
      </ul>
    ) : null}
    {query ? (
      <div
        className="btn btn-sm btn-default dlcsv"
        onClick={getDataForCSV(query, errorThrown)}
      >
        <span className="icon download dlcsv" />
        .csv
      </div>
    ) : null}
  </div>
)

const {arrayOf, func, shape, string} = PropTypes

VisHeader.propTypes = {
  views: arrayOf(string).isRequired,
  view: string.isRequired,
  onToggleView: func.isRequired,
  query: shape(),
  errorThrown: func.isRequired,
}

export default VisHeader
