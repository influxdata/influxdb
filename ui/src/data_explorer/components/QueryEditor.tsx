import React, {PureComponent} from 'react'

import {Controlled as ReactCodeMirror, IInstance} from 'react-codemirror2'
import {EditorChange} from 'codemirror'
import Dropdown from 'src/shared/components/Dropdown'
import classnames from 'classnames'
import {QUERY_TEMPLATES, QueryTemplate} from 'src/data_explorer/constants'
import QueryStatus from 'src/shared/components/QueryStatus'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {QueryConfig} from 'src/types'
import 'src/external/codemirror'

interface Props {
  query: string
  config: QueryConfig
  onUpdate: (value: string) => void
}

interface State {
  value: string
  focused: boolean
}

@ErrorHandling
class QueryEditor extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      value: this.props.query,
      focused: false,
    }
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

    const options = {
      tabIndex: 1,
      mode: 'influxQL',
      readonly: false,
      lineNumbers: false,
      autoRefresh: true,
      theme: 'influxql',
      completeSingle: false,
      lineWrapping: true,
    }

    return (
      <div className="query-editor">
        <div className={this.queryCodeClassName}>
          <ReactCodeMirror
            autoFocus={true}
            autoCursor={true}
            value={value}
            options={options}
            onBeforeChange={this.updateCode}
            onChange={this.handleChange}
            onTouchStart={this.onTouchStart}
            onBlur={this.handleBlur}
            onFocus={this.handleFocus}
            onKeyUp={this.handleKeyUp}
          />
        </div>
        <div className={this.varmojiClassName}>
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
                <button
                  className="btn btn-xs btn-primary query-editor--submit"
                  onClick={this.handleSubmit}
                >
                  Submit Query
                </button>
              </QueryStatus>
            </div>
          </div>
        </div>
      </div>
    )
  }

  private handleSubmit = (): void => {
    this.props.onUpdate(this.state.value)
  }

  private get queryCodeClassName(): string {
    const {focused} = this.state

    return classnames('query-editor--code', {focus: focused})
  }

  private get varmojiClassName(): string {
    const {focused} = this.state

    return classnames('varmoji', {focus: focused})
  }

  private onTouchStart = () => {}

  private handleChange = (): void => {}

  private handleBlur = (): void => {
    this.setState({focused: false})
    this.handleSubmit()
  }

  private handleFocus = (): void => {
    this.setState({focused: true})
  }

  private handleChooseMetaQuery = (template: QueryTemplate): void => {
    this.setState({value: template.query})
  }

  private handleKeyUp = (__, e: KeyboardEvent) => {
    const {ctrlKey, metaKey, key} = e

    if (key === 'Enter' && (ctrlKey || metaKey)) {
      this.handleSubmit()
    }
  }

  private updateCode = (
    _: IInstance,
    __: EditorChange,
    value: string
  ): void => {
    this.setState({value})
  }
}

export default QueryEditor
