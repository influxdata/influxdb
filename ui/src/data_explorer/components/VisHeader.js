import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'
import resultsToCSV from 'src/shared/parsing/resultsToCSV.js'
import download from 'src/external/download.js'

const getCSV = query => async () => {
  try {
    const {results} = await fetchTimeSeriesAsync({source: query.host, query})
    const {name, CSVString} = resultsToCSV(results)
    download(CSVString, `${name}.csv`, 'text/plain')
  } catch (error) {
    console.error(error)
  }
}

const VisHeader = ({views, view, onToggleView, name, query}) =>
  <div className="graph-heading">
    {views.length
      ? <div>
          <ul className="nav nav-tablist nav-tablist-sm">
            {views.map(v =>
              <li
                key={v}
                onClick={onToggleView(v)}
                className={classnames({active: view === v})}
                data-test={`data-${v}`}
              >
                {_.upperFirst(v)}
              </li>
            )}
          </ul>
          <div className="btn btn-sm btn-default dlcsv" onClick={getCSV(query)}>
            <span className="icon download dlcsv" />
            .csv
          </div>
        </div>
      : null}
    <div className="graph-title">
      {name}
    </div>
  </div>

const {arrayOf, func, shape, string} = PropTypes

VisHeader.propTypes = {
  views: arrayOf(string).isRequired,
  view: string.isRequired,
  onToggleView: func.isRequired,
  name: string.isRequired,
  query: shape().isRequired,
}

export default VisHeader
