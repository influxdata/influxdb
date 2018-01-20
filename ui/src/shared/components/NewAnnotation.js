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
    padding: '16px',
    textAlign: 'center',
    backgroundColor: 'rgba(255,0,0,0.2)',
    color: '#fff',
    borderRadius: '4px',
    fontSize: '16px',
    fontWeight: '400',
    opacity: isMouseHovering ? '0' : '1',
    transition: 'opacity 0.25s ease',
  }
}

class NewAnnotation extends Component {
  state = {
    xPos: null,
    isMouseHovering: false,
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
    console.log('mouseMove')
    this.setState({xPos: trueGraphX})
  }

  handleMouseLeave = () => {
    console.log('mouseLeave')
    this.setState({xPos: null, isMouseHovering: false})
  }

  handleClick = () => {
    const {onAddAnnotation, dygraph} = this.props
    const {xPos} = this.state

    const time = dygraph.toDataXCoord(xPos)

    const annotation = {
      id: 'newannotationid',
      group: '',
      name: 'New Annotation',
      time,
      duration: '',
      text: '',
    }
    console.log(annotation)
    onAddAnnotation(annotation)
    this.setState({xPos: null, isMouseHovering: false})
  }

  render() {
    // const {dygraph} = this.props
    const {xPos, isMouseHovering} = this.state

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
            <strong>Click</strong> to add Annotation
            <br />
            <strong>Drag</strong> to add Range
          </div>
        </div>
        <div className="new-annotation--crosshair" style={newLineStyle(xPos)}>
          <div className="new-annotation--tooltip" />
        </div>
      </div>
    )
  }
}

const {func, shape} = PropTypes

NewAnnotation.propTypes = {
  dygraph: shape({}).isRequired,
  onAddAnnotation: func.isRequired,
  onCancelAddAnnotation: func.isRequired,
}

export default NewAnnotation
