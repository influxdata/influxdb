import React, {Component, PropTypes} from 'react'

const calcStyle = ({time}, dygraph, isDragging) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()
  let visibility = 'visible'

  if (time < startX || time > endX) {
    visibility = 'hidden'
  }

  const containerLeftPadding = 16
  const left = `${dygraph.toDomXCoord(time) + containerLeftPadding}px`
  const width = 2

  return {
    left,
    position: 'absolute',
    top: '8px',
    backgroundColor: '#f00',
    height: 'calc(100% - 36px)',
    width: `${width}px`,
    transform: `translateX(-${width / 2}px)`, // translate should always be half with width to horizontally center the annotation pole
    visibility,
    zIndex: isDragging ? '4' : '3',
  }
}

// const triangleStyle = {
//   position: 'absolute',
//   top: '-5px',
//   left: '-6px',
//   border: '7px solid transparent',
//   borderTopColor: '#f00',
//   transform: 'scaleY(1.5)',
//   cursor: 'pointer',
// }
const triangleStyle = {
  position: 'absolute',
  top: '-7px',
  left: '-7px',
  width: '14px',
  height: '14px',
  backgroundColor: '#f00',
  cursor: 'pointer',
}
const triangleStyleDragging = {
  ...triangleStyle,
  backgroundColor: 'rgba(0,0,255,0.2)',
  width: '10000px',
  left: '-5000px',
  height: '10000px',
  top: '-5000px',
}

class Annotation extends Component {
  state = {
    isDragging: false,
  }

  handleStartDrag = () => {
    this.setState({isDragging: true})
  }

  handleStopDrag = () => {
    this.setState({isDragging: false})
  }

  handleMouseLeave = () => {
    this.setState({isDragging: false})
  }

  handleDrag = e => {
    if (!this.state.isDragging) {
      return
    }
    const {pageX} = e
    const {annotation, dygraph, onUpdateAnnotation} = this.props
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let time = dygraph.toDataXCoord(graphX)

    const minPercentChange = 0.5

    if (
      Math.abs(
        dygraph.toPercentXCoord(time) - dygraph.toPercentXCoord(annotation.time)
      ) *
        100 <
      minPercentChange
    ) {
      return
    }

    if (time >= endX) {
      time = endX
    }

    if (time <= startX) {
      time = startX
    }

    onUpdateAnnotation({...annotation, time: `${time}`})

    e.preventDefault()
    e.stopPropagation()
  }

  render() {
    const {annotation, dygraph} = this.props
    const {isDragging} = this.state

    return (
      <div
        className="dygraph-annotation"
        style={calcStyle(annotation, dygraph, isDragging)}
        data-time-ms={annotation.time}
        data-time-local={new Date(+annotation.time)}
      >
        <div
          style={isDragging ? triangleStyleDragging : triangleStyle}
          onMouseMove={this.handleDrag}
          onMouseDown={this.handleStartDrag}
          onMouseUp={this.handleStopDrag}
        />
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

Annotation.propTypes = {
  annotation: shape({
    id: string.isRequired,
    time: string.isRequired,
    duration: string,
  }).isRequired,
  dygraph: shape({}).isRequired,
  onUpdateAnnotation: func.isRequired,
}

export default Annotation
