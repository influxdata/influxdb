import React, {SFC} from 'react'
import {uniq, flatten} from 'lodash'
import {HistogramTooltipProps} from 'src/minard'
import {format} from 'd3-format'

import 'src/shared/components/HistogramTooltip.scss'

const formatLarge = format('.4~s')
const formatSmall = format('.4~g')
const formatBin = n => (n < 1 && n > -1 ? formatSmall(n) : formatLarge(n))

const HistogramTooltip: SFC<HistogramTooltipProps> = ({xMin, xMax, counts}) => {
  const groupColNames = uniq(
    flatten(counts.map(({grouping}) => Object.keys(grouping)))
  ).sort()

  return (
    <div className="histogram-tooltip">
      <div className="histogram-tooltip--bin">
        {formatBin(xMin)} &ndash; {formatBin(xMax)}
      </div>
      <div className="histogram-tooltip--table">
        {groupColNames.map(groupColName => (
          <div key={groupColName} className="histogram-tooltip--fills">
            <div className="histogram-tooltip--column-header">
              {groupColName}
            </div>
            {counts.map(({grouping, color}) => (
              <div
                className="histogram-tooltip--fill"
                key={color}
                style={{color}}
              >
                {grouping[groupColName]}
              </div>
            ))}
          </div>
        ))}
        <div className="histogram-tooltip--counts">
          <div className="histogram-tooltip--column-header">count</div>
          {counts.map(({count, color}) => (
            <div
              className="histogram-tooltip--count"
              key={color}
              style={{color}}
            >
              {count}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default HistogramTooltip
