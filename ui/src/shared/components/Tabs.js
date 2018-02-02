import React, {PropTypes} from 'react'
import classnames from 'classnames'

const {node, func, bool, number, string} = PropTypes
export const Tab = React.createClass({
  propTypes: {
    children: node.isRequired,
    onClick: func,
    isDisabled: bool,
    isActive: bool,
    isKapacitorTab: bool,
    isConfigured: bool,
  },

  render() {
    if (this.props.isKapacitorTab) {
      return (
        <li
          className={classnames({active: this.props.isActive})}
          onClick={this.props.isDisabled ? null : this.props.onClick}
        >
          {this.props.children}
        </li>
      )
    }
    return (
      <div
        className={classnames('btn tab', {
          active: this.props.isActive,
          configured: this.props.isConfigured,
        })}
        onClick={this.props.isDisabled ? null : this.props.onClick}
      >
        {this.props.children}
      </div>
    )
  },
})

export const TabList = React.createClass({
  propTypes: {
    children: node.isRequired,
    activeIndex: number,
    onActivate: func,
    isKapacitorTabs: string,
    customClass: string,
  },

  getDefaultProps() {
    return {
      isKapacitorTabs: '',
    }
  },

  render() {
    const children = React.Children.map(this.props.children, (child, index) => {
      return React.cloneElement(child, {
        isActive: index === this.props.activeIndex,
        onClick: () => this.props.onActivate(index),
      })
    })

    if (this.props.isKapacitorTabs === 'true') {
      return (
        <div className="rule-section--row rule-section--row-first rule-section--row-last">
          <p>Choose One:</p>
          <div className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
            {children}
          </div>
        </div>
      )
    }

    if (this.props.customClass) {
      return (
        <div className={this.props.customClass}>
          <div className="btn-group btn-group-lg tab-group">
            {children}
          </div>
        </div>
      )
    }

    return (
      <div className="btn-group btn-group-lg tab-group">
        {children}
      </div>
    )
  },
})

export const TabPanels = React.createClass({
  propTypes: {
    children: node.isRequired,
    activeIndex: number,
    customClass: string,
  },

  // if only 1 child, children array index lookup will fail
  render() {
    return (
      <div className={this.props.customClass ? this.props.customClass : null}>
        {this.props.children[this.props.activeIndex]}
      </div>
    )
  },
})

export const TabPanel = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  render() {
    return (
      <div>
        {this.props.children}
      </div>
    )
  },
})

export const Tabs = React.createClass({
  propTypes: {
    children: node.isRequired,
    onSelect: func,
    tabContentsClass: string,
    tabsClass: string,
    initialIndex: number,
  },

  getDefaultProps() {
    return {
      onSelect() {},
      tabContentsClass: '',
    }
  },

  getInitialState() {
    // initialIndex allows the user to enable a Tab and TabPanel
    // other than 0 on initial render.
    return {
      activeIndex: this.props.initialIndex || 0,
    }
  },

  handleActivateTab(activeIndex) {
    this.setState({activeIndex})
    this.props.onSelect(activeIndex)
  },

  render() {
    const children = React.Children.map(this.props.children, child => {
      if (child && child.type === TabPanels) {
        return React.cloneElement(child, {
          activeIndex: this.state.activeIndex,
        })
      }

      if (child && child.type === TabList) {
        return React.cloneElement(child, {
          activeIndex: this.state.activeIndex,
          onActivate: this.handleActivateTab,
        })
      }

      return child
    })

    return (
      <div className={this.props.tabContentsClass}>
        {children}
      </div>
    )
  },
})
