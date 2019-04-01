import React, {useRef, SFC} from 'react'
import {createPortal} from 'react-dom'
import {uniq, flatten} from 'lodash'
import {HistogramTooltipProps, useTooltipStyle} from '@influxdata/vis'
import {format} from 'd3-format'

import {TOOLTIP_PORTAL_ID} from 'src/shared/components/TooltipPortal'

const formatLarge = format('.4~s')
const formatSmall = format('.4~g')
const formatBin = n => (n < 1 && n > -1 ? formatSmall(n) : formatLarge(n))

const HistogramTooltip: SFC<HistogramTooltipProps> = ({xMin, xMax, counts}) => {
  const tooltipEl = useRef<HTMLDivElement>(null)

  useTooltipStyle(tooltipEl.current)

  const groupColNames = uniq(
    flatten(counts.map(({grouping}) => Object.keys(grouping)))
  ).sort()

  return createPortal(
    <div className="histogram-tooltip" ref={tooltipEl}>
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
    </div>,
    document.querySelector(`#${TOOLTIP_PORTAL_ID}`)
  )
}

export default HistogramTooltip
