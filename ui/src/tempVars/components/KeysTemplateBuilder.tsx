import React, {PureComponent} from 'react'

import {getDeep} from 'src/utils/wrappers'
import {ErrorHandling} from 'src/shared/decorators/errors'
import Dropdown from 'src/shared/components/Dropdown'
import {showDatabases, showMeasurements} from 'src/shared/apis/metaQuery'
import parseShowDatabases from 'src/shared/parsing/showDatabases'
import parseShowMeasurements from 'src/shared/parsing/showMeasurements'
import TemplateMetaQueryPreview from 'src/tempVars/components/TemplateMetaQueryPreview'
import DropdownLoadingPlaceholder from 'src/shared/components/DropdownLoadingPlaceholder'

import {
  TemplateBuilderProps,
  TemplateValueType,
  RemoteDataState,
  Source,
} from 'src/types'

interface Props extends TemplateBuilderProps {
  queryPrefix: string
  templateValueType: TemplateValueType
  fetchKeys: (
    source: Source,
    db: string,
    measurement: string
  ) => Promise<string[]>
}

interface State {
  databases: string[]
  databasesStatus: RemoteDataState
  selectedDatabase: string
  measurements: string[]
  measurementsStatus: RemoteDataState
  selectedMeasurement: string
  keys: string[]
  keysStatus: RemoteDataState
}

@ErrorHandling
class KeysTemplateBuilder extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    const selectedDatabase = getDeep(props, 'template.query.db', '')
    const selectedMeasurement = getDeep(props, 'template.query.measurement', '')

    this.state = {
      databases: [],
      databasesStatus: RemoteDataState.Loading,
      selectedDatabase,
      measurements: [],
      measurementsStatus: RemoteDataState.Loading,
      selectedMeasurement,
      keys: [],
      keysStatus: RemoteDataState.Loading,
    }
  }

  public async componentDidMount() {
    await this.loadDatabases()
    await this.loadMeasurements()
    await this.loadKeys()
  }

  public render() {
    const {queryPrefix} = this.props
    const {
      databases,
      databasesStatus,
      selectedDatabase,
      measurements,
      measurementsStatus,
      selectedMeasurement,
      keys,
      keysStatus,
    } = this.state

    return (
      <div className="temp-builder measurements-temp-builder">
        <div className="form-group">
          <label>Meta Query</label>
          <div className="temp-builder--mq-controls">
            <div className="temp-builder--mq-text">{queryPrefix}</div>
            <DropdownLoadingPlaceholder rds={databasesStatus}>
              <Dropdown
                items={databases.map(text => ({text}))}
                onChoose={this.handleChooseDatabaseDropdown}
                selected={selectedDatabase}
                buttonSize=""
              />
            </DropdownLoadingPlaceholder>
            <div className="temp-builder--mq-text">FROM</div>
            <DropdownLoadingPlaceholder rds={measurementsStatus}>
              <Dropdown
                items={measurements.map(text => ({text}))}
                onChoose={this.handleChooseMeasurementDropdown}
                selected={selectedMeasurement}
                buttonSize=""
              />
            </DropdownLoadingPlaceholder>
          </div>
        </div>
        <TemplateMetaQueryPreview items={keys} loadingStatus={keysStatus} />
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
        this.handleChooseDatabase(getDeep(databases, 0, ''))
      }
    } catch (error) {
      this.setState({databasesStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private async loadMeasurements(): Promise<void> {
    const {source} = this.props
    const {selectedDatabase, selectedMeasurement} = this.state

    this.setState({measurementsStatus: RemoteDataState.Loading})

    try {
      const {data} = await showMeasurements(
        source.links.proxy,
        selectedDatabase
      )
      const {measurementSets} = parseShowMeasurements(data)
      const measurements = getDeep(measurementSets, '0.measurements', [])

      this.setState({
        measurements,
        measurementsStatus: RemoteDataState.Done,
      })

      if (!selectedMeasurement) {
        this.handleChooseMeasurement(getDeep(measurements, 0, ''))
      }
    } catch (error) {
      this.setState({measurementsStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private async loadKeys(): Promise<void> {
    const {
      template,
      onUpdateTemplate,
      templateValueType,
      fetchKeys,
      source,
    } = this.props

    const {selectedDatabase, selectedMeasurement} = this.state

    this.setState({keysStatus: RemoteDataState.Loading})

    try {
      const keys = await fetchKeys(
        source,
        selectedDatabase,
        selectedMeasurement
      )

      this.setState({
        keys,
        keysStatus: RemoteDataState.Done,
      })

      const nextValues = keys.map(value => {
        return {
          type: templateValueType,
          value,
          selected: false,
        }
      })

      if (nextValues[0]) {
        nextValues[0].selected = true
      }

      onUpdateTemplate({...template, values: nextValues})
    } catch (error) {
      this.setState({keysStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private handleChooseDatabaseDropdown = ({text}) => {
    this.handleChooseDatabase(text)
  }

  private handleChooseDatabase = (db: string): void => {
    this.setState({selectedDatabase: db, selectedMeasurement: ''}, () =>
      this.loadMeasurements()
    )

    const {template, onUpdateTemplate} = this.props

    onUpdateTemplate({
      ...template,
      query: {
        ...template.query,
        db,
      },
    })
  }

  private handleChooseMeasurementDropdown = ({text}): void => {
    this.handleChooseMeasurement(text)
  }

  private handleChooseMeasurement = (measurement: string): void => {
    this.setState({selectedMeasurement: measurement}, () => this.loadKeys())

    const {template, onUpdateTemplate} = this.props

    onUpdateTemplate({
      ...template,
      query: {
        ...template.query,
        measurement,
      },
    })
  }
}

export default KeysTemplateBuilder
