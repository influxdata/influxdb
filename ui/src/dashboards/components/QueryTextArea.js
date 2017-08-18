import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

import FillQuery from 'shared/components/FillQuery'
import TemplateDrawer from 'shared/components/TemplateDrawer'
import QueryStatus from 'shared/components/QueryStatus'

import {
  MATCH_INCOMPLETE_TEMPLATES,
  applyMasks,
  insertTempVar,
  unMask,
} from 'src/dashboards/constants'

class QueryTextArea extends Component {
  constructor(props) {
    super(props)
    this.state = {
      value: this.props.query,
      isTemplating: false,
      selectedTemplate: {
        tempVar: _.get(this.props.templates, ['0', 'tempVar'], ''),
      },
      filteredTemplates: this.props.templates,
    }
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.query !== nextProps.query) {
      this.setState({value: nextProps.query})
    }
  }

  handleCloseDrawer = () => {
    this.setState({isTemplating: false})
  }

  handleMouseOverTempVar = template => () => {
    this.handleTemplateReplace(template)
  }

  handleClickTempVar = template => () => {
    // Clicking a tempVar does the same thing as hitting 'Enter'
    this.handleTemplateReplace(template, true)
    this.closeDrawer()
  }

  closeDrawer = () => {
    this.setState({
      isTemplating: false,
      selectedTemplate: {
        tempVar: _.get(this.props.templates, ['0', 'tempVar'], ''),
      },
    })
  }

  handleFillQuery = (val, isNum) => {
    console.log(val, isNum)
  }

  handleKeyDown = e => {
    const {isTemplating, value} = this.state

    if (isTemplating) {
      switch (e.key) {
        case 'Tab':
        case 'ArrowRight':
        case 'ArrowDown':
          e.preventDefault()
          return this.handleTemplateReplace(this.findTempVar('next'))
        case 'ArrowLeft':
        case 'ArrowUp':
          e.preventDefault()
          return this.handleTemplateReplace(this.findTempVar('previous'))
        case 'Enter':
          e.preventDefault()
          this.handleTemplateReplace(this.state.selectedTemplate, true)
          return this.closeDrawer()
        case 'Escape':
          e.preventDefault()
          return this.closeDrawer()
      }
    } else if (e.key === 'Escape') {
      e.preventDefault()
      this.setState({value, isTemplating: false})
    } else if (e.key === 'Enter') {
      e.preventDefault()
      this.handleUpdate()
    }
  }

  handleTemplateReplace = (selectedTemplate, replaceWholeTemplate) => {
    const {selectionStart, value} = this.editor
    const {tempVar} = selectedTemplate
    const newTempVar = replaceWholeTemplate
      ? tempVar
      : tempVar.substring(0, tempVar.length - 1)

    // mask matches that will confuse our regex
    const masked = applyMasks(value)
    const matched = masked.match(MATCH_INCOMPLETE_TEMPLATES)

    let templatedValue
    if (matched) {
      templatedValue = insertTempVar(masked, newTempVar)
      templatedValue = unMask(templatedValue)
    }

    const enterModifier = replaceWholeTemplate ? 0 : -1
    const diffInLength =
      tempVar.length - _.get(matched, '0', []).length + enterModifier

    this.setState({value: templatedValue, selectedTemplate}, () =>
      this.editor.setSelectionRange(
        selectionStart + diffInLength,
        selectionStart + diffInLength
      )
    )
  }

  findTempVar = direction => {
    const {filteredTemplates: templates} = this.state
    const {selectedTemplate} = this.state

    const i = _.findIndex(templates, selectedTemplate)
    const lastIndex = templates.length - 1

    if (i >= 0) {
      if (direction === 'next') {
        return templates[(i + 1) % templates.length]
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

  handleChange = () => {
    const {templates} = this.props
    const {selectedTemplate} = this.state
    const value = this.editor.value

    // mask matches that will confuse our regex
    const masked = applyMasks(value)
    const matched = masked.match(MATCH_INCOMPLETE_TEMPLATES)

    if (matched && !_.isEmpty(templates)) {
      // maintain cursor poition
      const start = this.editor.selectionStart

      const end = this.editor.selectionEnd
      const filterText = matched[0].substr(1).toLowerCase()

      const filteredTemplates = templates.filter(t =>
        t.tempVar.toLowerCase().includes(filterText)
      )

      const found = filteredTemplates.find(
        t => t.tempVar === selectedTemplate && selectedTemplate.tempVar
      )
      const newTemplate = found ? found : filteredTemplates[0]

      this.setState({
        isTemplating: true,
        selectedTemplate: newTemplate,
        filteredTemplates,
        value,
      })
      this.editor.setSelectionRange(start, end)
    } else {
      this.setState({isTemplating: false, value})
    }
  }

  handleUpdate = () => {
    this.props.onUpdate(this.state.value)
  }

  handleSelectTempVar(tempVar) {
    this.setState({selectedTemplate: tempVar})
  }

  render() {
    const {config: {status}} = this.props
    const {
      value,
      isTemplating,
      selectedTemplate,
      filteredTemplates,
    } = this.state

    return (
      <div className="query-editor">
        <textarea
          className="query-editor--field"
          onChange={this.handleChange}
          onKeyDown={this.handleKeyDown}
          onBlur={this.handleUpdate}
          ref={editor => (this.editor = editor)}
          value={value}
          placeholder="Enter a query or select database, measurement, and field below and have us build one for you..."
          autoComplete="off"
          spellCheck="false"
          data-test="query-editor-field"
        />
        <div
          className={classnames('varmoji', {'varmoji-rotated': isTemplating})}
        >
          <div className="varmoji-container">
            <div className="varmoji-front">
              <QueryStatus status={status}>
                <FillQuery onSelection={this.handleFillQuery} />
              </QueryStatus>
            </div>
            <div className="varmoji-back">
              {isTemplating
                ? <TemplateDrawer
                    onClickTempVar={this.handleClickTempVar}
                    templates={filteredTemplates}
                    selected={selectedTemplate}
                    onMouseOverTempVar={this.handleMouseOverTempVar}
                    handleClickOutside={this.handleCloseDrawer}
                  />
                : null}
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

QueryTextArea.propTypes = {
  query: string.isRequired,
  onUpdate: func.isRequired,
  config: shape().isRequired,
  isInDataExplorer: bool,
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ),
}

export default QueryTextArea
