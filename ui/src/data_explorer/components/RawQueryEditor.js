import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import classNames from 'classnames'

import Dropdown from 'src/shared/components/Dropdown'
import LoadingDots from 'src/shared/components/LoadingDots'
import TemplateDrawer from 'src/shared/components/TemplateDrawer'
import {QUERY_TEMPLATES} from 'src/data_explorer/constants'
import {TEMPLATE_MATCHER} from 'src/dashboards/constants'

class RawQueryEditor extends Component {
  constructor(props) {
    super(props)
    this.state = {
      value: this.props.query,
      isTemplating: false,
      selectedTempVar: {
        tempVar: '',
      },
    }

    this.handleKeyDown = ::this.handleKeyDown
    this.handleChange = ::this.handleChange
    this.handleUpdate = ::this.handleUpdate
    this.handleChooseTemplate = ::this.handleChooseTemplate
    this.handleCloseDrawer = ::this.handleCloseDrawer
    this.findTempVar = ::this.findTempVar
    this.handleTemplateReplace = ::this.handleTemplateReplace
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.query !== nextProps.query) {
      this.setState({value: nextProps.query})
    }
  }

  handleCloseDrawer() {
    this.setState({isTemplating: false})
  }

  handleKeyDown(e) {
    const {isTemplating, value} = this.state

    if (isTemplating) {
      if (e.key === ('ArrowRight' || 'ArrowDown')) {
        e.preventDefault()

        this.handleTemplateReplace('next')
      }

      if (e.key === ('ArrowLeft' || 'ArrowUp')) {
        e.preventDefault()
        this.handleTemplateReplace('previous')
      }

      if (e.key === 'Enter') {
        e.preventDefault()
        const start = this.editor.selectionStart
        const end = this.editor.selectionEnd
        this.editor.setSelectionRange(start, end)
      }
    } else if (e.key === 'Escape') {
      e.preventDefault()
      this.setState({value, isTemplating: false})
    } else if (e.key === 'Enter') {
      this.handleUpdate()
    }
  }

  handleTemplateReplace(direction) {
    const {selectionStart, value} = this.editor
    const selectedTempVar = this.findTempVar(direction)

    let templatedValue
    const matched = value.match(TEMPLATE_MATCHER)
    if (matched) {
      templatedValue = value.replace(
        TEMPLATE_MATCHER,
        `:${selectedTempVar.tempVar}`
      )
    }

    // debugger
    const diffInLength = selectedTempVar.tempVar.length - matched[0].length
    // console.log(diffInLength)

    this.setState({value: templatedValue, selectedTempVar}, () =>
      this.editor.setSelectionRange(
        selectionStart + diffInLength + 1,
        selectionStart + diffInLength + 1
      )
    )
  }

  findTempVar(direction) {
    const {templates} = this.props
    const {selectedTempVar} = this.state

    const i = _.findIndex(templates, selectedTempVar)
    const lastIndex = templates.length - 1

    if (i >= 0) {
      if (direction === 'next') {
        if (i === lastIndex) {
          return templates[0]
        }
        return templates[i + 1]
      }

      if (direction === 'previous') {
        if (i === 0) {
          return templates[lastIndex]
        }
        return templates[i - 1]
      }
    }

    return templates[0]
  }

  handleChange() {
    const value = this.editor.value

    if (value.match(TEMPLATE_MATCHER)) {
      // maintain cursor poition
      const start = this.editor.selectionStart
      const end = this.editor.selectionEnd
      this.setState({isTemplating: true, value})
      this.editor.setSelectionRange(start, end)
    } else {
      this.setState({isTemplating: false, value})
    }
  }

  handleUpdate() {
    this.props.onUpdate(this.state.value)
  }

  handleChooseTemplate(template) {
    this.setState({value: template.query})
  }

  handleSelectTempVar(tempVar) {
    this.setState({selectedTempVar: tempVar})
  }

  render() {
    const {config: {status}, templates} = this.props
    const {value, isTemplating, selectedTempVar} = this.state

    return (
      <div className="raw-text">
        {isTemplating
          ? <TemplateDrawer
              templates={templates}
              selected={selectedTempVar}
              handleClickOutside={this.handleCloseDrawer}
            />
          : null}
        <textarea
          className="raw-text--field"
          onChange={this.handleChange}
          onKeyDown={this.handleKeyDown}
          onBlur={this.handleUpdate}
          ref={editor => (this.editor = editor)}
          value={value}
          placeholder="Enter a query or select database, measurement, and field below and have us build one for you..."
          autoComplete="off"
          spellCheck="false"
        />
        {this.renderStatus(status)}
        <Dropdown
          items={QUERY_TEMPLATES}
          selected={'Query Templates'}
          onChoose={this.handleChooseTemplate}
          className="query-template"
        />
      </div>
    )
  }

  renderStatus(status) {
    if (!status) {
      return <div className="raw-text--status" />
    }

    if (status.loading) {
      return (
        <div className="raw-text--status">
          <LoadingDots />
        </div>
      )
    }

    return (
      <div
        className={classNames('raw-text--status', {
          'raw-text--error': status.error,
          'raw-text--success': status.success,
          'raw-text--warning': status.warn,
        })}
      >
        <span
          className={classNames('icon', {
            stop: status.error,
            checkmark: status.success,
            'alert-triangle': status.warn,
          })}
        />
        {status.error || status.warn || status.success}
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

RawQueryEditor.propTypes = {
  query: string.isRequired,
  onUpdate: func.isRequired,
  config: shape().isRequired,
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ).isRequired,
}

export default RawQueryEditor
