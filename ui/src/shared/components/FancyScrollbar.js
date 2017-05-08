import React, {Component, PropTypes} from 'react'
import {Scrollbars} from 'react-custom-scrollbars'

class FancyScrollbox extends Component {
  constructor(props) {
    super(props)
  }

  // static defaultProps = {
  //   className: 'fancy-scroll--container',
  // }

  render() {
    const {children, className} = this.props

    return (
      <Scrollbars
        className={`fancy-scroll--container ${className}`}
        autoHide={true}
        autoHideTimeout={1000}
        autoHideDuration={250}
        renderTrackHorizontal={props => <div {...props} className="fancy-scroll--track-h"/>}
        renderTrackVertical={props => <div {...props} className="fancy-scroll--track-v"/>}
        renderThumbHorizontal={props => <div {...props} className="fancy-scroll--thumb-h"/>}
        renderThumbVertical={props => <div {...props} className="fancy-scroll--thumb-v"/>}
      >
        {children}
      </Scrollbars>
    )
  }
}

const {node, string} = PropTypes

FancyScrollbox.propTypes = {
  children: node.isRequired,
  className: string.isRequired,
}

export default FancyScrollbox
