import React, {Component, PropTypes} from 'react'

const newContainer = {
  position: 'absolute',
  zIndex: '9999',
  top: '8px',
  left: '16px',
  width: 'calc(100% - 32px)',
  height: 'calc(100% - 16px)',
  backgroundColor: 'rgba(255,255,255,0.2)',
}
const newLineStyle = left => {
  const width = 2

  return {
    position: 'absolute',
    top: '0',
    left: `${left}px`,
    height: 'calc(100% - 20px)',
    width: `${width}px`,
    transform: `translateX(-${width / 2}px)`, // translate should always be half with width to horizontally center the comment pole
    backgroundColor: '#f00',
    visibility: left ? 'visible' : 'hidden',
    zIndex: '5',
  }
}
const prompterContainerStyle = isMouseHovering => {
  return {
    height: '100%',
    display: 'flex',
    justifyContent: 'center',
    alignContent: 'center',
    alignItems: 'center',
    visibility: isMouseHovering ? 'hidden' : 'visible',
    transition: 'all',
  }
}

const prompterStyle = isMouseHovering => {
  return {
    padding: '16px 32px',
    textAlign: 'center',
    backgroundColor: 'rgba(255,0,0,0.7)',
    color: '#fff',
    borderRadius: '5px',
    fontSize: '17px',
    lineHeight: '30px',
    fontWeight: '400',
    opacity: isMouseHovering ? '0' : '1',
    transition: 'opacity 0.25s ease',
  }
}

class NewAnnotation extends Component {
  state = {
    xPos: null,
    isMouseHovering: true,
  }

  handleMouseEnter = () => {
    this.setState({isMouseHovering: true})
  }

  handleMouseMove = e => {
    if (this.state.isMouseHovering === false) {
      return
    }

    const wrapperRect = this.wrapper.getBoundingClientRect()
    const trueGraphX = e.pageX - wrapperRect.left
    this.setState({xPos: trueGraphX})
  }

  handleMouseLeave = () => {
    this.setState({xPos: null, isMouseHovering: false})
  }

  handleClick = () => {
    const {onAddAnnotation, onAddingAnnotationSuccess, dygraph} = this.props
    const {xPos} = this.state

    const time = `${dygraph.toDataXCoord(xPos)}`

    const annotation = {
      id: 'newannotationid', // TODO generate real ID
      group: '',
      name: 'New Annotation',
      time,
      duration: '',
      text: '',
    }

    this.setState({xPos: null, isMouseHovering: false})
    onAddingAnnotationSuccess()
    onAddAnnotation(annotation)
  }

  render() {
    const {dygraph} = this.props
    const {xPos, isMouseHovering} = this.state

    const timestamp = `${new Date(dygraph.toDataXCoord(xPos))}`

    return (
      <div
        className="new-annotation"
        ref={el => (this.wrapper = el)}
        onMouseMove={this.handleMouseMove}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
        onClick={this.handleClick}
        style={newContainer}
      >
        <div
          className="new-annotation--prompter"
          style={prompterContainerStyle(isMouseHovering)}
        >
          <div style={prompterStyle(isMouseHovering)}>
            <strong>Click</strong> to create Annotation
            <br />
            <strong>Drag</strong> to create Range
          </div>
        </div>
        <div className="new-annotation--crosshair" style={newLineStyle(xPos)}>
          <div className="new-annotation--tooltip">
            Create Annotation at:
            <br />
            {timestamp}
          </div>
        </div>
      </div>
    )
  }
}

const {func, shape} = PropTypes

NewAnnotation.propTypes = {
  dygraph: shape({}).isRequired,
  onAddAnnotation: func.isRequired,
  onAddingAnnotationSuccess: func.isRequired,
}

export default NewAnnotation
