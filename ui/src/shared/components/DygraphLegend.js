import React, {PropTypes} from 'react'

const DygraphLegend = ({series}) => (
  <div style={{userSelect: 'text'}} className="container--dygraph-legend">
    <div className="dygraph-legend--header">
      <input className="form-control input-xs" type="text" />
      <button className="btn btn-primary btn-xs">
        A-Z
      </button>
      <button className="btn btn-primary btn-xs">
        0-9
      </button>
    </div>
    <div className="dygraph-legend--contents">
      {series.map(({label, color, yHTML}) => {
        return (
          <span key={label + color}>
            <b>
              <span style={{color}}>
                {label}: {yHTML}
              </span>
            </b>
          </span>
        )
      })}
    </div>
  </div>
)

const {arrayOf, bool, number, shape, string} = PropTypes

DygraphLegend.propTypes = {
  x: number,
  xHTML: string,
  series: arrayOf(
    shape({
      color: string,
      dashHTML: string,
      isVisible: bool,
      label: string,
      y: number,
      yHTML: string,
    })
  ),
  dygraph: shape(),
}

export default DygraphLegend
