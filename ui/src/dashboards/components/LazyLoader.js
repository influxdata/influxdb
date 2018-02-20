import React, {Component, PropTypes} from 'react'

class LazyLoader extends React.Component {
  state = {
    availableHeight: 0,
    scrollTop: 0,
  }

  componentDidMount() {
    this.setHeight()
    window.addEventListener('resize', this.setHeight, true)
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.setHeight, true)
  }

  handleScroll = event => {
    this.setState({
      scrollTop: event.target.scrollTop,
    })
  }

  setHeight = () => {
    this.setState({
      availableHeight: this.node.clientHeight,
    })
  }

  render() {
    // const {numRows, rowHeight, renderRowAtIndex} = this.props
    // const totalHeight = rowHeight * numRows

    const {availableHeight, scrollTop} = this.state
    const scrollBottom = scrollTop + availableHeight
    console.dir(availableHeight)

    // const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - 20)
    // const endIndex = Math.min(numRows, Math.ceil(scrollBottom / rowHeight) + 20)

    // const items = []

    // let index = startIndex
    // while (index < endIndex) {
    //   items.push(
    //     <li key={index}>
    //       {renderRowAtIndex(index)}
    //     </li>
    //   )
    //   index++
    // }

    return <div />
  }
}

const {func, number} = PropTypes
LazyLoader.proptypes = {
  //   numRows: PropTypes.number.isRequired,
  //   rowHeight: PropTypes.number.isRequired,
  //   renderRowAtIndex: PropTypes.func.isRequired,
}

export default LazyLoader
