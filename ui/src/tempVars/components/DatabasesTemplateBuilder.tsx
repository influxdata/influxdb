import React, {PureComponent} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {showDatabases} from 'src/shared/apis/metaQuery'
import parseShowDatabases from 'src/shared/parsing/showDatabases'
import TemplateMetaQueryPreview from 'src/tempVars/components/TemplateMetaQueryPreview'

import {
  TemplateBuilderProps,
  RemoteDataState,
  TemplateValueType,
} from 'src/types'

interface State {
  databasesStatus: RemoteDataState
}

@ErrorHandling
class DatabasesTemplateBuilder extends PureComponent<
  TemplateBuilderProps,
  State
> {
  constructor(props) {
    super(props)

    this.state = {
      databasesStatus: RemoteDataState.Loading,
    }
  }

  public async componentDidMount() {
    this.loadDatabases()
  }

  public render() {
    const {template, onUpdateDefaultTemplateValue} = this.props
    const {databasesStatus} = this.state

    return (
      <div className="temp-builder databases-temp-builder">
        <div className="form-group">
          <label>Meta Query</label>
          <div className="temp-builder--mq-controls">
            <div className="temp-builder--mq-text">SHOW DATABASES</div>
          </div>
        </div>
        <TemplateMetaQueryPreview
          items={template.values}
          loadingStatus={databasesStatus}
          onUpdateDefaultTemplateValue={onUpdateDefaultTemplateValue}
        />
      </div>
    )
  }

  private async loadDatabases(): Promise<void> {
    const {template, source, onUpdateTemplate} = this.props

    this.setState({databasesStatus: RemoteDataState.Loading})

    try {
      const {data} = await showDatabases(source.links.proxy)
      const {databases} = parseShowDatabases(data)

      this.setState({databasesStatus: RemoteDataState.Done})

      const nextValues = databases.map(db => {
        return {
          type: TemplateValueType.Database,
          value: db,
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
      }

      onUpdateTemplate(nextTemplate)
    } catch {
      this.setState({databasesStatus: RemoteDataState.Error})
    }
  }
}

export default DatabasesTemplateBuilder
