import React, {PropTypes, Component} from 'react'

import Dropdown from 'shared/components/Dropdown'
import {QUERY_TEMPLATES} from 'src/data_explorer/constants'
import QueryStatus from 'shared/components/QueryStatus'

class QueryEditor extends Component {
  constructor(props) {
    super(props)
    this.state = {
      value: this.props.query,
    }

    this.handleKeyDown = ::this.handleKeyDown
    this.handleChange = ::this.handleChange
    this.handleUpdate = ::this.handleUpdate
    this.handleChooseMetaQuery = ::this.handleChooseMetaQuery
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.query !== nextProps.query) {
      this.setState({value: nextProps.query})
    }
  }

  handleKeyDown(e) {
    const {value} = this.state

    if (e.key === 'Escape') {
      e.preventDefault()
      this.setState({value})
    }

    if (e.key === 'Enter') {
      e.preventDefault()
      this.handleUpdate()
    }
  }

  handleChange() {
    this.setState({value: this.editor.value})
  }

  handleUpdate() {
    this.props.onUpdate(this.state.value)
  }

  handleChooseMetaQuery(template) {
    this.setState({value: template.query})
  }

  render() {
    const {config: {status}} = this.props
    const {value} = this.state

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
        <div className="varmoji">
          <div className="varmoji-container">
            <div className="varmoji-front">
              <QueryStatus status={status}>
                <Dropdown
                  items={QUERY_TEMPLATES}
                  selected={'Query Templates'}
                  onChoose={this.handleChooseMetaQuery}
                  className="dropdown-140 query-editor--templates"
                  buttonSize="btn-xs"
                />
              </QueryStatus>
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

QueryEditor.propTypes = {
  query: string.isRequired,
  onUpdate: func.isRequired,
  config: shape().isRequired,
}

export default QueryEditor
