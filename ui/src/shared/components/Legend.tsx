// Libraries
import React, {PureComponent, CSSProperties} from 'react'
import {createPortal} from 'react-dom'
import moment from 'moment'
import {uniq, flatten, isNumber} from 'lodash'

// Components
import DapperScrollbars from 'src/shared/components/dapperScrollbars/DapperScrollbars'

// Constants
import {LEGEND_PORTAL_ID} from 'src/shared/components/LegendPortal'
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'

// Types
import {SeriesDescription} from 'src/shared/parsing/flux/spreadTables'

interface Props {
  seriesDescriptions: SeriesDescription[]
  time: number
  values: {[seriesKey: string]: number}
  x: number
  colors: {[seriesKey: string]: string}
  visRect: DOMRect
}

const VALUE_PRECISION = 2
const NOISY_COLUMNS = new Set(['_start', '_stop', 'result'])
const LEGEND_MARGIN = 10

class Legend extends PureComponent<Props> {
  private legendRef = React.createRef<HTMLDivElement>()

  public componentDidMount() {
    this.setPosition()
  }

  public componentDidUpdate() {
    this.setPosition()
  }

  public render() {
    return createPortal(
      <div className="legend" ref={this.legendRef}>
        <div className="legend--time">{this.time}</div>
        <DapperScrollbars style={{maxHeight: '120px'}} autoHide={true}>
          <div className="legend--columns">
            {this.columns.map(({name, isNumeric, rows}, i) => (
              <div
                key={`${name}-${i}`}
                className={`legend--column ${isNumeric ? 'numeric' : ''}`}
              >
                <div className="legend--column-header">{name}</div>
                {rows.map(({color, value}, j) => {
                  const emptyClass = !value && value !== 0 ? 'empty' : ''

                  return (
                    <div
                      key={`${color}-${j}`}
                      className={`legend--column-row ${emptyClass}`}
                      style={{color}}
                    >
                      {isNumber(value) ? value.toFixed(VALUE_PRECISION) : value}
                    </div>
                  )
                })}
              </div>
            ))}
          </div>
        </DapperScrollbars>
      </div>,
      document.querySelector(`#${LEGEND_PORTAL_ID}`)
    )
  }

  private get time() {
    return moment(this.props.time).format(DEFAULT_TIME_FORMAT)
  }

  private get columns() {
    const {seriesDescriptions, values, colors} = this.props

    const metaColumnNames = uniq(
      flatten(seriesDescriptions.map(d => Object.keys(d.metaColumns)))
    )

    let columns = metaColumnNames.map(name => ({
      name,
      isNumeric: false,
      rows: [],
    }))

    const descsByKey = seriesDescriptions.reduce(
      (acc, d) => ({...acc, [d.key]: d}),
      {}
    )

    const seriesKeys = Object.keys(values)

    // Sort series so that those with higher values are first
    seriesKeys.sort((a, b) => values[b] - values[a])

    for (const column of columns) {
      for (const key of seriesKeys) {
        if (!descsByKey[key]) {
          // Guard against an edge case where the `values` prop (originating
          // from the Dygraphs library) becomes out of sync from the
          // `seriesDescriptions` prop. This will happen if the legend is
          // displayed while a graph is rerendered with new time series. In
          // this case we render nothing and wait for the next `values` prop to
          // trigger a valid rerender.
          return []
        }

        column.rows.push({
          color: colors[key],
          value: descsByKey[key].metaColumns[column.name],
        })
      }
    }

    // Don't show noisy columns like `_start` and `_stop` if the value for every
    // series in that column will be the same
    columns = columns.filter(
      col =>
        !NOISY_COLUMNS.has(col.name) ||
        !col.rows.every(d => d.value === col.rows[0].value)
    )

    const valueColumnNames = uniq(
      seriesDescriptions.map(d => d.valueColumnName)
    )

    for (const name of valueColumnNames) {
      const rows = seriesKeys.map(key => {
        let value

        if (descsByKey[key].valueColumnName === name) {
          value = values[key]
        }

        return {value, color: colors[key]}
      })

      columns.push({name, rows, isNumeric: true})
    }

    return columns
  }

  private get style(): CSSProperties {
    const {x, visRect} = this.props
    const legendRect = this.legendRef.current.getBoundingClientRect()

    let left = x + visRect.left - legendRect.width / 2

    if (left + legendRect.width > window.innerWidth - LEGEND_MARGIN) {
      left = window.innerWidth - legendRect.width - LEGEND_MARGIN
    } else if (left < LEGEND_MARGIN) {
      left = LEGEND_MARGIN
    }

    let top = visRect.top - legendRect.height

    if (top < LEGEND_MARGIN) {
      top = visRect.bottom
    }

    if (top + legendRect.height > window.innerHeight - LEGEND_MARGIN) {
      top = LEGEND_MARGIN
    }

    const style: CSSProperties = {
      left: `${left}px`,
      top: `${top}px`,
    }

    return style
  }

  private setPosition(): void {
    if (!this.legendRef.current) {
      return
    }

    // We update the placement of the legend by mutating it's `style` attribute
    // after it has rendered, so that we can use the size of the legend to
    // calculate a reasonable placement
    const styleAttr = Object.entries(this.style).reduce(
      (acc, [k, v]) => `${acc}; ${k}: ${v}`,
      ''
    )

    this.legendRef.current.setAttribute('style', styleAttr)
  }
}

export default Legend
