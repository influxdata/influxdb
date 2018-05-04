import _ from 'lodash'
import React, {Component} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import {Scrollbars} from 'react-custom-scrollbars'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class FancyScrollbar extends Component {
  constructor(props) {
    super(props)
  }

  static defaultProps = {
    autoHide: true,
    autoHeight: false,
    setScrollTop: () => {},
  }

  updateScroll() {
    if (this.ref && _.isNumber(this.props.scrollTop)) {
      this.ref.scrollTop(this.props.scrollTop)
    }

    if (this.ref && _.isNumber(this.props.scrollLeft)) {
      this.ref.scrollLeft(this.props.scrollLeft)
    }
  }

  componentDidMount() {
    this.updateScroll()
  }

  componentDidUpdate() {
    this.updateScroll()
  }

  handleMakeDiv = className => props => {
    return <div {...props} className={`fancy-scroll--${className}`} />
  }

  onRef = ref => {
    this.ref = ref
  }

  render() {
    const {
      autoHide,
      autoHeight,
      children,
      className,
      maxHeight,
      setScrollTop,
      style,
    } = this.props

    return (
      <Scrollbars
        className={classnames('fancy-scroll--container', {
          [className]: className,
        })}
        ref={this.onRef}
        style={style}
        onScroll={setScrollTop}
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

const {bool, func, node, number, string, object} = PropTypes

FancyScrollbar.propTypes = {
  children: node.isRequired,
  className: string,
  autoHide: bool,
  autoHeight: bool,
  maxHeight: number,
  setScrollTop: func,
  style: object,
  scrollTop: number,
  scrollLeft: number,
}

export default FancyScrollbar
