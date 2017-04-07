import React, {PropTypes} from 'react'
import classNames from 'classnames'
import Dropdown from 'src/shared/components/Dropdown'
import LoadingDots from 'src/shared/components/LoadingDots'
import {QUERY_TEMPLATES} from 'src/data_explorer/constants'

const ENTER = 13
const ESCAPE = 27
const {
  func,
  shape,
  string,
} = PropTypes
const RawQueryEditor = React.createClass({
  propTypes: {
    query: string.isRequired,
    onUpdate: func.isRequired,
    config: shape().isRequired,
  },

  getInitialState() {
    return {
      value: this.props.query,
    }
  },

  componentWillReceiveProps(nextProps) {
    if (this.props.query !== nextProps.query) {
      this.setState({value: nextProps.query})
    }
  },

  handleKeyDown(e) {
    if (e.keyCode === ENTER) {
      e.preventDefault()
      this.handleUpdate()
    } else if (e.keyCode === ESCAPE) {
      this.setState({value: this.state.value}, () => {
        this.editor.blur()
      })
    }
  },

  handleChange() {
    this.setState({
      value: this.editor.value,
    })
  },

  handleUpdate() {
    this.props.onUpdate(this.state.value)
  },

  handleChooseTemplate(template) {
    this.setState({value: template.query})
  },

  render() {
    const {config: {status}} = this.props
    const {value} = this.state

    return (
      <div className="raw-text">
        <textarea
          className="raw-text--field"
          onChange={this.handleChange}
          onKeyDown={this.handleKeyDown}
          onBlur={this.handleUpdate}
          ref={(editor) => this.editor = editor}
          value={value}
          placeholder="Enter a query or select database, measurement, and field below and have us build one for you..."
          autoComplete="off"
          spellCheck="false"
        />
        {this.renderStatus(status)}
        <Dropdown items={QUERY_TEMPLATES} selected={'Query Templates'} onChoose={this.handleChooseTemplate} className="query-template"/>
      </div>
    )
  },

  renderStatus(status) {
    if (!status) {
      return (
        <div className="raw-text--status"></div>
      )
    }

    if (status.loading) {
      return (
        <div className="raw-text--status">
          <LoadingDots />
        </div>
      )
    }

    return (
      <div className={classNames("raw-text--status", {"raw-text--error": status.error, "raw-text--success": status.success, "raw-text--warning": status.warn})}>
        <span className={classNames("icon", {stop: status.error, checkmark: status.success, "alert-triangle": status.warn})}></span>
        {status.error || status.warn || status.success}
      </div>
    )
  },
})

export default RawQueryEditor
