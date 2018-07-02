import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'

import {ErrorHandling} from 'src/shared/decorators/errors'
import TemplateMetaQueryPreview from 'src/tempVars/components/TemplateMetaQueryPreview'
import {hydrateTemplate} from 'src/tempVars/apis'
import {isInvalidMetaQuery} from 'src/tempVars/parsing'

import {TemplateBuilderProps, RemoteDataState} from 'src/types'

const DEBOUNCE_DELAY = 750

interface State {
  metaQueryInput: string // bound to input
  metaQuery: string // debounced view of metaQueryInput
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
      metaQueryResultsStatus: RemoteDataState.NotStarted,
    }
  }

  public componentDidMount() {
    this.executeQuery()
  }

  public render() {
    const {metaQueryInput} = this.state

    return (
      <>
        <div className="form-group col-xs-12">
          <label>Meta Query</label>
          <div className="temp-builder--mq-controls">
            <textarea
              className="form-control input-sm"
              value={metaQueryInput}
              onChange={this.handleMetaQueryInputChange}
              onBlur={this.handleMetaQueryChange}
            />
          </div>
        </div>
        {this.renderResults()}
      </>
    )
  }

  private renderResults() {
    const {template, onUpdateDefaultTemplateValue} = this.props
    const {metaQueryResultsStatus} = this.state

    if (this.showInvalidMetaQueryMessage) {
      return (
        <div className="form-group col-xs-12 temp-builder--results">
          <p className="temp-builder--validation error">
            Meta Query is not valid.
          </p>
        </div>
      )
    }

    return (
      <TemplateMetaQueryPreview
        items={template.values}
        loadingStatus={metaQueryResultsStatus}
        onUpdateDefaultTemplateValue={onUpdateDefaultTemplateValue}
      />
    )
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
    const {template, templates, source, onUpdateTemplate} = this.props
    const {metaQuery} = this.state

    if (this.isInvalidMetaQuery) {
      return
    }

    this.setState({metaQueryResultsStatus: RemoteDataState.Loading})

    try {
      const templateWithQuery = {
        ...template,
        query: {influxql: metaQuery},
      }

      const nextTemplate = await hydrateTemplate(
        source.links.proxy,
        templateWithQuery,
        templates
      )

      this.setState({metaQueryResultsStatus: RemoteDataState.Done})

      if (nextTemplate.values[0]) {
        nextTemplate.values[0].selected = true
      }

      onUpdateTemplate(nextTemplate)
    } catch {
      this.setState({
        metaQueryResultsStatus: RemoteDataState.Error,
      })
    }
  }
}

export default CustomMetaQueryTemplateBuilder
