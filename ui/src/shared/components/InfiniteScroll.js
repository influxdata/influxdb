import React, {Component} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import {Scrollbars} from 'react-custom-scrollbars'
import _ from 'lodash'
import {ErrorHandling} from 'src/shared/decorators/errors'

const {arrayOf, number, shape, string} = PropTypes

@ErrorHandling
class InfiniteScroll extends Component {
  // Cache values from Scrollbars events that need to be independent of render
  // Should not be setState as need not trigger a re-render
  scrollbarsScrollTop = 0
  scrollbarsClientHeight = 0

  state = {
    topIndex: 0,
    bottomIndex: 0,
    topPadding: 0,
    bottomPadding: 0,
    windowHeight: window.innerHeight,
  }

  windowing = (props, state) => {
    const {itemHeight, items} = props
    const {bottomIndex} = state

    const itemDistance = Math.round(this.scrollbarsScrollTop / itemHeight)
    const itemCount = Math.round(this.scrollbarsClientHeight / itemHeight) + 1

    // If state is the same, do not setState to the same value multiple times.
    // Improves performance and prevents errors.
    if (bottomIndex === itemDistance + itemCount) {
      return
    }

    this.setState({
      // Number of items from top
      topIndex: itemDistance,
      // Number of items that can fit inside the container div
      bottomIndex: itemDistance + itemCount,
      // Offset list from top
      topPadding: itemDistance * itemHeight,
      // Provide scrolling room at the bottom of the list
      bottomPadding: (items.length - itemDistance - itemCount) * itemHeight,
    })
  }

  handleScroll = ({clientHeight, scrollTop}) => {
    let shouldUpdate = false

    if (
      (typeof clientHeight !== 'undefined' &&
        this.scrollbarsClientHeight !== clientHeight) ||
      (typeof scrollTop !== 'undefined' &&
        this.scrollbarsScrollTop !== scrollTop)
    ) {
      shouldUpdate = true
    }

    this.scrollbarsClientHeight = clientHeight
    this.scrollbarsScrollTop = scrollTop

    if (shouldUpdate) {
      this.windowing(this.props, this.state)
    }
  }

  throttledHandleScroll = _.throttle(this.handleScroll, 100)

  handleResize = () => {
    this.setState({windowHeight: window.innerHeight})
  }

  throttledHandleResize = _.throttle(this.handleResize, 100)

  handleMakeDiv = className => props => (
    <div {...props} className={`fancy-scroll--${className}`} />
  )

  componentDidMount() {
    window.addEventListener('resize', this.handleResize, true)
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.handleResize, true)
  }

  componentWillReceiveProps(nextProps, nextState) {
    // Updates values if new items are added
    this.windowing(nextProps, nextState)
  }

  render() {
    const {className, items} = this.props
    const {
      topIndex,
      bottomIndex,
      topPadding,
      bottomPadding,
      windowHeight,
    } = this.state

    return (
      <Scrollbars
        className={classnames(className, 'fancy-scroll--container')}
        autoHide={true}
        autoHideTimeout={1000}
        autoHideDuration={250}
        autoHeight={false}
        renderTrackHorizontal={this.handleMakeDiv('track-h')}
        renderTrackVertical={this.handleMakeDiv('track-v')}
        renderThumbHorizontal={this.handleMakeDiv('thumb-h')}
        renderThumbVertical={this.handleMakeDiv('thumb-v')}
        renderView={this.handleMakeDiv('view')}
        onScrollFrame={this.throttledHandleScroll}
        onUpdate={this.throttledHandleScroll}
        key={windowHeight}
      >
        <div style={{height: topPadding}} />
        {items.filter((_item, i) => i >= topIndex && i <= bottomIndex)}
        <div style={{height: bottomPadding}} />
      </Scrollbars>
    )
  }
}

InfiniteScroll.propTypes = {
  itemHeight: number.isRequired,
  items: arrayOf(shape()).isRequired,
  className: string,
}

export default InfiniteScroll
