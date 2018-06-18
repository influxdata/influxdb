import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import Dropdown from 'src/shared/components/Dropdown'
import {showDatabases, showMeasurements} from 'src/shared/apis/metaQuery'
import parseShowDatabases from 'src/shared/parsing/showDatabases'
import parseShowMeasurements from 'src/shared/parsing/showMeasurements'
import TemplateMetaQueryPreview from 'src/tempVars/components/TemplateMetaQueryPreview'
import DropdownLoadingPlaceholder from 'src/shared/components/DropdownLoadingPlaceholder'

import {
  TemplateBuilderProps,
  RemoteDataState,
  TemplateValueType,
} from 'src/types'

interface State {
  databases: string[]
  databasesStatus: RemoteDataState
  selectedDatabase: string
  measurements: string[]
  measurementsStatus: RemoteDataState
}

@ErrorHandling
class MeasurementsTemplateBuilder extends PureComponent<
  TemplateBuilderProps,
  State
> {
  constructor(props) {
    super(props)

    const selectedDatabase = _.get(props, 'template.query.db', '')

    this.state = {
      databases: [],
      databasesStatus: RemoteDataState.Loading,
      selectedDatabase,
      measurements: [],
      measurementsStatus: RemoteDataState.Loading,
    }
  }

  public async componentDidMount() {
    await this.loadDatabases()
    await this.loadMeasurements()
  }

  public render() {
    const {
      databases,
      databasesStatus,
      selectedDatabase,
      measurements,
      measurementsStatus,
    } = this.state

    return (
      <div className="temp-builder measurements-temp-builder">
        <div className="form-group">
          <label>Meta Query</label>
          <div className="temp-builder--mq-controls">
            <div className="temp-builder--mq-text">SHOW MEASUREMENTS ON</div>
            <DropdownLoadingPlaceholder rds={databasesStatus}>
              <Dropdown
                items={databases.map(text => ({text}))}
                onChoose={this.handleChooseDatabaseDropdown}
                selected={selectedDatabase}
                buttonSize=""
              />
            </DropdownLoadingPlaceholder>
          </div>
        </div>
        <TemplateMetaQueryPreview
          items={measurements}
          loadingStatus={measurementsStatus}
        />
      </div>
    )
  }

  private async loadDatabases(): Promise<void> {
    const {source} = this.props

    this.setState({databasesStatus: RemoteDataState.Loading})

    try {
      const {data} = await showDatabases(source.links.proxy)
      const {databases} = parseShowDatabases(data)
      const {selectedDatabase} = this.state

      this.setState({
        databases,
        databasesStatus: RemoteDataState.Done,
      })

      if (!selectedDatabase) {
        this.handleChooseDatabase(_.get(databases, 0, ''))
      }
    } catch (error) {
      this.setState({databasesStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private async loadMeasurements(): Promise<void> {
    const {template, source, onUpdateTemplate} = this.props
    const {selectedDatabase} = this.state

    this.setState({measurementsStatus: RemoteDataState.Loading})

    try {
      const {data} = await showMeasurements(
        source.links.proxy,
        selectedDatabase
      )
      const {measurementSets} = parseShowMeasurements(data)

      const measurements = _.get(measurementSets, '0.measurements', [])

      this.setState({
        measurements,
        measurementsStatus: RemoteDataState.Done,
      })

      const nextValues = measurements.map(value => {
        return {
          type: TemplateValueType.Measurement,
          value,
          selected: false,
        }
      })

      if (nextValues[0]) {
        nextValues[0].selected = true
      }

      onUpdateTemplate({...template, values: nextValues})
    } catch (error) {
      this.setState({measurementsStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private handleChooseDatabaseDropdown = ({text}) => {
    this.handleChooseDatabase(text)
  }

  private handleChooseDatabase = (db: string): void => {
    this.setState({selectedDatabase: db}, () => this.loadMeasurements())

    const {template, onUpdateTemplate} = this.props

    onUpdateTemplate({
      ...template,
      query: {
        ...template.query,
        db,
      },
    })
  }
}

export default MeasurementsTemplateBuilder
