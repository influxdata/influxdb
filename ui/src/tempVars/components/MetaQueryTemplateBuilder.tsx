import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'

import {proxy} from 'src/utils/queryUrlGenerator'
import {ErrorHandling} from 'src/shared/decorators/errors'
import TemplateMetaQueryPreview from 'src/tempVars/components/TemplateMetaQueryPreview'
import {parseMetaQuery, isInvalidMetaQuery} from 'src/tempVars/utils/parsing'

import {
  TemplateBuilderProps,
  RemoteDataState,
  TemplateValueType,
} from 'src/types'

const DEBOUNCE_DELAY = 750

interface State {
  metaQueryInput: string // bound to input
  metaQuery: string // debounced view of metaQueryInput
  metaQueryResults: string[]
  metaQueryResultsStatus: RemoteDataState
}

@ErrorHandling
class CustomMetaQueryTemplateBuilder extends PureComponent<
  TemplateBuilderProps,
  State
> {
  private handleMetaQueryChange: () => void = _.debounce(() => {
    const {metaQuery, metaQueryInput} = this.state

    if (metaQuery === metaQueryInput) {
      return
    }

    this.setState({metaQuery: metaQueryInput}, this.executeQuery)
  }, DEBOUNCE_DELAY)

  constructor(props: TemplateBuilderProps) {
    super(props)

    const metaQuery = getDeep<string>(props.template, 'query.influxql', '')

    this.state = {
      metaQuery,
      metaQueryInput: metaQuery,
      metaQueryResults: [],
      metaQueryResultsStatus: RemoteDataState.NotStarted,
    }
  }

  public componentDidMount() {
    this.executeQuery()
  }

  public render() {
    const {metaQueryInput} = this.state

    return (
      <div className="temp-builder csv-temp-builder">
        <div className="form-group">
          <label>Meta Query</label>
          <div className="temp-builder--mq-controls">
            <textarea
              className="form-control"
              value={metaQueryInput}
              onChange={this.handleMetaQueryInputChange}
              onBlur={this.handleMetaQueryChange}
            />
          </div>
        </div>
        {this.renderResults()}
      </div>
    )
  }

  private renderResults() {
    const {onUpdateDefaultTemplateValue} = this.props
    const {metaQueryResults, metaQueryResultsStatus} = this.state

    if (this.showInvalidMetaQueryMessage) {
      return (
        <div className="temp-builder-results">
          <p className="error">Meta Query is not valid.</p>
        </div>
      )
    }

    return (
      <TemplateMetaQueryPreview
        items={metaQueryResults}
        loadingStatus={metaQueryResultsStatus}
        defaultValue={this.defaultValue}
        onUpdateDefaultTemplateValue={onUpdateDefaultTemplateValue}
      />
    )
  }
  private get defaultValue(): string {
    const {template} = this.props
    const defaultTemplateValue = template.values.find(v => v.default)
    return getDeep<string>(defaultTemplateValue, 'value', '')
  }

  private get showInvalidMetaQueryMessage(): boolean {
    const {metaQuery} = this.state

    return this.isInvalidMetaQuery && metaQuery !== ''
  }

  private get isInvalidMetaQuery(): boolean {
    const {metaQuery} = this.state

    return isInvalidMetaQuery(metaQuery)
  }

  private handleMetaQueryInputChange = (
    e: ChangeEvent<HTMLTextAreaElement>
  ) => {
    this.setState({metaQueryInput: e.target.value})
    this.handleMetaQueryChange()
  }

  private executeQuery = async (): Promise<void> => {
    const {template, source, onUpdateTemplate} = this.props
    const {metaQuery} = this.state

    if (this.isInvalidMetaQuery) {
      return
    }

    this.setState({metaQueryResultsStatus: RemoteDataState.Loading})

    try {
      const {data} = await proxy({
        source: source.links.proxy,
        query: metaQuery,
      })

      const metaQueryResults = parseMetaQuery(metaQuery, data)

      this.setState({
        metaQueryResults,
        metaQueryResultsStatus: RemoteDataState.Done,
      })

      const nextValues = metaQueryResults.map(result => {
        return {
          type: TemplateValueType.MetaQuery,
          value: result,
          selected: false,
          default: false,
        }
      })

      if (nextValues[0]) {
        nextValues[0].selected = true
      }

      const nextTemplate = {
        ...template,
        values: nextValues,
        query: {
          influxql: metaQuery,
        },
      }

      onUpdateTemplate(nextTemplate)
    } catch {
      this.setState({
        metaQueryResults: [],
        metaQueryResultsStatus: RemoteDataState.Error,
      })
    }
  }
}

export default CustomMetaQueryTemplateBuilder
