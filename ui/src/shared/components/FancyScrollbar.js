import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import {Scrollbars} from 'react-custom-scrollbars'

class FancyScrollbar extends Component {
  constructor(props) {
    super(props)
  }

  static defaultProps = {
    autoHide: true,
    autoHeight: false,
    getScrollTop: () => {},
  }

  handleMakeDiv = className => props => {
    return <div {...props} className={`fancy-scroll--${className}`} />
  }

  render() {
    const {
      autoHide,
      autoHeight,
      children,
      className,
      maxHeight,
      getScrollTop,
    } = this.props

    return (
      <Scrollbars
        className={classnames('fancy-scroll--container', {
          [className]: className,
        })}
        onScroll={getScrollTop}
        autoHide={autoHide}
        autoHideTimeout={1000}
        autoHideDuration={250}
        autoHeight={autoHeight}
        autoHeightMax={maxHeight}
        renderTrackHorizontal={this.handleMakeDiv('track-h')}
        renderTrackVertical={this.handleMakeDiv('track-v')}
        renderThumbHorizontal={this.handleMakeDiv('thumb-h')}
        renderThumbVertical={this.handleMakeDiv('thumb-v')}
        renderView={this.handleMakeDiv('view')}
      >
        {children}
      </Scrollbars>
    )
  }
}

const {bool, func, node, number, string} = PropTypes

FancyScrollbar.propTypes = {
  children: node.isRequired,
  className: string,
  autoHide: bool,
  autoHeight: bool,
  maxHeight: number,
  getScrollTop: func,
}

export default FancyScrollbar
