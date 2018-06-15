import React, {PureComponent, KeyboardEvent} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import {QUERY_TEMPLATES, QueryTemplate} from 'src/data_explorer/constants'
import QueryStatus from 'src/shared/components/QueryStatus'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {QueryConfig} from 'src/types'

interface Props {
  query: string
  config: QueryConfig
  onUpdate: (value: string) => void
}

interface State {
  value: string
}

@ErrorHandling
class QueryEditor extends PureComponent<Props, State> {
  private editor: React.RefObject<HTMLTextAreaElement>

  constructor(props: Props) {
    super(props)
    this.state = {
      value: this.props.query,
    }

    this.editor = React.createRef<HTMLTextAreaElement>()
  }

  public componentWillReceiveProps(nextProps: Props) {
    if (this.props.query !== nextProps.query) {
      this.setState({value: nextProps.query})
    }
  }

  public render() {
    const {
      config: {status},
    } = this.props
    const {value} = this.state

    return (
      <div className="query-editor">
        <textarea
          className="query-editor--field"
          ref={this.editor}
          value={value}
          autoComplete="off"
          spellCheck={false}
          onBlur={this.handleUpdate}
          onChange={this.handleChange}
          onKeyDown={this.handleKeyDown}
          data-test="query-editor-field"
          placeholder="Enter a query or select database, measurement, and field below and have us build one for you..."
        />
        <div className="varmoji">
          <div className="varmoji-container">
            <div className="varmoji-front">
              <QueryStatus status={status}>
                <Dropdown
                  items={QUERY_TEMPLATES}
                  selected="Query Templates"
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

  private handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>): void => {
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

  private handleChange = (): void => {
    const value = this.editor.current.value
    this.setState({value})
  }

  private handleUpdate = (): void => {
    this.props.onUpdate(this.state.value)
  }

  private handleChooseMetaQuery = (template: QueryTemplate): void => {
    this.setState({value: template.query})
  }
}

export default QueryEditor
