import React, {SFC} from 'react'
import {HistogramTooltipProps} from 'src/minard'
import {format} from 'd3-format'

import 'src/shared/components/HistogramTooltip.scss'

const formatLarge = format('.4~s')
const formatSmall = format('.4~g')
const formatBin = n => (n < 1 && n > -1 ? formatSmall(n) : formatLarge(n))

const HistogramTooltip: SFC<HistogramTooltipProps> = ({
  fill,
  xMin,
  xMax,
  counts,
}) => {
  return (
    <div className="histogram-tooltip">
      <div className="histogram-tooltip--bin">
        {formatBin(xMin)} &ndash; {formatBin(xMax)}
      </div>
      <div className="histogram-tooltip--table">
        {fill && (
          <div className="histogram-tooltip--fills">
            <div className="histogram-tooltip--column-header">{fill}</div>
            {counts.map(({fill: fillDatum, color}) => (
              <div key={color} style={{color}}>
                {fillDatum}
              </div>
            ))}
          </div>
        )}
        <div className="histogram-tooltip--counts">
          <div className="histogram-tooltip--column-header">count</div>
          {counts.map(({count, color}) => (
            <div key={color} style={{color}}>
              {count}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default HistogramTooltip
