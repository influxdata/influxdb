import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import Dropdown from 'src/shared/components/Dropdown'
import {showDatabases, showMeasurements} from 'src/shared/apis/metaQuery'
import {proxy} from 'src/utils/queryUrlGenerator'
import parseShowDatabases from 'src/shared/parsing/showDatabases'
import parseShowMeasurements from 'src/shared/parsing/showMeasurements'
import parseShowTagValues from 'src/shared/parsing/showTagValues'
import {fetchTagKeys} from 'src/tempVars/components/TagKeysTemplateBuilder'
import TemplateMetaQueryPreview from 'src/tempVars/components/TemplateMetaQueryPreview'
import DropdownLoadingPlaceholder from 'src/shared/components/DropdownLoadingPlaceholder'

import {
  TemplateBuilderProps,
  TemplateValueType,
  RemoteDataState,
} from 'src/types'

interface State {
  databases: string[]
  databasesStatus: RemoteDataState
  selectedDatabase: string
  measurements: string[]
  measurementsStatus: RemoteDataState
  selectedMeasurement: string
  tagKeys: string[]
  tagKeysStatus: RemoteDataState
  selectedTagKey: string
  tagValues: string[]
  tagValuesStatus: RemoteDataState
}

@ErrorHandling
class KeysTemplateBuilder extends PureComponent<TemplateBuilderProps, State> {
  constructor(props) {
    super(props)

    const selectedDatabase = _.get(props, 'template.query.db', '')
    const selectedMeasurement = _.get(props, 'template.query.measurement', '')
    const selectedTagKey = _.get(props, 'template.query.tagKey', '')

    this.state = {
      databases: [],
      databasesStatus: RemoteDataState.Loading,
      selectedDatabase,
      measurements: [],
      measurementsStatus: RemoteDataState.Loading,
      selectedMeasurement,
      tagKeys: [],
      tagKeysStatus: RemoteDataState.Loading,
      selectedTagKey,
      tagValues: [],
      tagValuesStatus: RemoteDataState.Loading,
    }
  }

  public async componentDidMount() {
    await this.loadDatabases()
    await this.loadMeasurements()
    await this.loadTagKeys()
    await this.loadTagValues()
  }

  public render() {
    const {
      databases,
      databasesStatus,
      selectedDatabase,
      measurements,
      measurementsStatus,
      selectedMeasurement,
      tagKeys,
      tagKeysStatus,
      selectedTagKey,
      tagValues,
      tagValuesStatus,
    } = this.state

    return (
      <div className="temp-builder measurements-temp-builder">
        <div className="form-group">
          <label>Meta Query</label>
          <div className="temp-builder--mq-controls">
            <div className="temp-builder--mq-text">SHOW TAG VALUES ON</div>
            <DropdownLoadingPlaceholder rds={databasesStatus}>
              <Dropdown
                items={databases.map(text => ({text}))}
                onChoose={this.handleChooseDatabaseDropdown}
                selected={selectedDatabase}
                buttonSize=""
              />
            </DropdownLoadingPlaceholder>
          </div>
          <div className="temp-builder--mq-controls">
            <div className="temp-builder--mq-text">FROM</div>
            <DropdownLoadingPlaceholder rds={measurementsStatus}>
              <Dropdown
                items={measurements.map(text => ({text}))}
                onChoose={this.handleChooseMeasurementDropdown}
                selected={selectedMeasurement}
                buttonSize=""
              />
            </DropdownLoadingPlaceholder>
            <div className="temp-builder--mq-text">WITH KEY</div>
            <DropdownLoadingPlaceholder rds={tagKeysStatus}>
              <Dropdown
                items={tagKeys.map(text => ({text}))}
                onChoose={this.handleChooseTagKeyDropdown}
                selected={selectedTagKey}
                buttonSize=""
              />
            </DropdownLoadingPlaceholder>
          </div>
        </div>
        <TemplateMetaQueryPreview
          items={tagValues}
          loadingStatus={tagValuesStatus}
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
    const {source} = this.props
    const {selectedDatabase, selectedMeasurement} = this.state

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

      if (!selectedMeasurement) {
        this.handleChooseMeasurement(_.get(measurements, 0, ''))
      }
    } catch (error) {
      this.setState({measurementsStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private async loadTagKeys(): Promise<void> {
    const {source} = this.props
    const {selectedTagKey} = this.state

    const {selectedDatabase, selectedMeasurement} = this.state

    this.setState({tagKeysStatus: RemoteDataState.Loading})

    try {
      const tagKeys = await fetchTagKeys(
        source,
        selectedDatabase,
        selectedMeasurement
      )

      this.setState({
        tagKeys,
        tagKeysStatus: RemoteDataState.Done,
      })

      if (!selectedTagKey) {
        this.handleChooseTagKey(_.get(tagKeys, 0, ''))
      }
    } catch (error) {
      this.setState({tagKeysStatus: RemoteDataState.Error})
      console.error(error)
    }
  }

  private loadTagValues = async (): Promise<void> => {
    const {source, template, onUpdateTemplate} = this.props
    const {selectedDatabase, selectedMeasurement, selectedTagKey} = this.state

    this.setState({tagValuesStatus: RemoteDataState.Loading})

    try {
      const {data} = await proxy({
        source: source.links.proxy,
        db: selectedDatabase,
        query: `SHOW TAG VALUES ON "${selectedDatabase}" FROM "${selectedMeasurement}" WITH KEY = "${selectedTagKey}"`,
      })

      const {tags} = parseShowTagValues(data)
      const tagValues = _.get(Object.values(tags), 0, [])

      this.setState({
        tagValues,
        tagValuesStatus: RemoteDataState.Done,
      })

      const nextValues = tagValues.map(value => {
        return {
          type: TemplateValueType.TagValue,
          value,
          selected: false,
        }
      })

      if (nextValues[0]) {
        nextValues[0].selected = true
      }

      onUpdateTemplate({...template, values: nextValues})
    } catch (error) {
      this.setState({tagValuesStatus: RemoteDataState.Error})
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
        tagKey: '',
        measurement: '',
      },
    })
  }

  private handleChooseMeasurementDropdown = ({text}): void => {
    this.handleChooseMeasurement(text)
  }

  private handleChooseMeasurement = (measurement: string): void => {
    this.setState({selectedMeasurement: measurement, selectedTagKey: ''}, () =>
      this.loadTagKeys()
    )

    const {template, onUpdateTemplate} = this.props

    onUpdateTemplate({
      ...template,
      query: {
        ...template.query,
        measurement,
        tagKey: '',
      },
    })
  }

  private handleChooseTagKeyDropdown = ({text}): void => {
    this.handleChooseTagKey(text)
  }

  private handleChooseTagKey = (tagKey: string): void => {
    this.setState({selectedTagKey: tagKey}, () => this.loadTagValues())

    const {template, onUpdateTemplate} = this.props

    onUpdateTemplate({
      ...template,
      query: {
        ...template.query,
        tagKey,
      },
    })
  }
}

export default KeysTemplateBuilder
