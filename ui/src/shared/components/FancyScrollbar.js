import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import {Scrollbars} from 'react-custom-scrollbars'

class FancyScrollbar extends Component {
  constructor(props) {
    super(props)
  }

  static defaultProps = {
    autoHide: true,
  }

  render() {
    const {autoHide, children, className} = this.props

    return (
      <Scrollbars
        className={classnames('fancy-scroll--container', {[className]: className})}
        autoHide={autoHide}
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

const {bool, node, string} = PropTypes

FancyScrollbar.propTypes = {
  children: node.isRequired,
  className: string,
  autoHide: bool,
}

export default FancyScrollbar