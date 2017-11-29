import React, {Component, PropTypes} from 'react'

const {arrayOf, number, shape, string} = PropTypes

class InfiniteScroll extends Component {
  scrollElement

  // Cache values that need to be independent of
  // Should not be setState as need not trigger a re-render
  scrollTop = 0
  containerHeight = 0

  static propTypes = {
    itemHeight: number.isRequired,
    items: arrayOf(shape()).isRequired,
    className: string,
  }

  state = {
    topIndex: 0,
    bottomIndex: 0,
    topPadding: 0,
    bottomPadding: 0,
  }

  windowing = (props, state) => {
    const {itemHeight, items} = props
    const {bottomIndex} = state

    const itemDistance = Math.round(this.scrollTop / itemHeight)
    const itemCount = Math.round(this.containerHeight / itemHeight) + 1

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

  handleScroll = evt => {
    if (evt.target === this.scrollElement) {
      this.scrollTop = evt.target.scrollTop
      this.windowing(this.props, this.state)
    }
  }

  handleResize = () => {
    this.containerHeight = this.scrollElement.clientHeight
  }

  componentDidMount() {
    this.containerHeight = this.scrollElement.clientHeight
    this.windowing(this.props, this.state)

    window.addEventListener('scroll', this.handleScroll, true)
    window.addEventListener('resize', this.handleResize, true)
  }

  componentWillUnmount() {
    window.removeEventListener('scroll', this.handleScroll, true)
    window.removeEventListener('resize', this.handleResize, true)
  }

  componentWillReceiveProps(nextProps, nextState) {
    // Updates values if new items are added
    this.windowing(nextProps, nextState)
  }

  render() {
    const {className, items} = this.props
    const {topIndex, bottomIndex, topPadding, bottomPadding} = this.state

    return (
      <div
        className={className}
        ref={r => (this.scrollElement = r)}
        style={{
          overflowY: 'scroll',
        }}
      >
        <div style={{height: topPadding}} />
        {items.filter((_item, i) => i >= topIndex && i <= bottomIndex)}
        <div style={{height: bottomPadding}} />
      </div>
    )
  }
}

export default InfiniteScroll
