// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import BuilderCard from 'src/shared/components/BuilderCard'

// APIs
import {
  findBuckets,
  findMeasurements,
  findFields,
  LIMIT,
} from 'src/shared/apis/v2/queryBuilder'

// Actions
import {buildQuery} from 'src/shared/actions/v2/timeMachines'

// Utils
import {restartable, CancellationError} from 'src/utils/restartable'
import {getActiveQuery} from 'src/shared/selectors/timeMachines'
import {getSources, getActiveSource} from 'src/sources/selectors'

// Constants
import {FUNCTIONS} from 'src/shared/constants/queryBuilder'

// Styles
import 'src/shared/components/TimeMachineQueryBuilder.scss'

// Types
import {RemoteDataState} from 'src/types'
import {AppState, Source, SourceType, BuilderConfig} from 'src/types/v2'

const EMPTY_FIELDS_MESSAGE = 'Select at least one bucket and measurement'
const EMPTY_FUNCTIONS_MESSAGE = 'Select at least one bucket and measurement'
const EMPTY_MEASUREMENTS_MESSAGE = 'Select a bucket'
const mergeUnique = (items: string[], selection: string[]) =>
  [...new Set([...items, ...selection])].sort()

interface StateProps {
  queryURL: string
  sourceType: SourceType
}

interface DispatchProps {
  onBuildQuery: typeof buildQuery
}

type Props = StateProps & DispatchProps

interface State {
  buckets: string[]
  bucketsStatus: RemoteDataState
  bucketsSelection: string[]
  measurements: string[]
  measurementsStatus: RemoteDataState
  measurementsSelection: string[]
  fields: string[]
  fieldsStatus: RemoteDataState
  fieldsSelection: string[]
  functions: string[]
  functionsStatus: RemoteDataState
  functionsSelection: string[]
}

class TimeMachineQueryBuilder extends PureComponent<Props, State> {
  public state: State = {
    buckets: [],
    bucketsStatus: RemoteDataState.Loading,
    bucketsSelection: [],
    measurements: [],
    measurementsStatus: RemoteDataState.NotStarted,
    measurementsSelection: [],
    fields: [],
    fieldsStatus: RemoteDataState.NotStarted,
    fieldsSelection: [],
    functions: FUNCTIONS.map(f => f.name),
    functionsStatus: RemoteDataState.NotStarted,
    functionsSelection: [],
  }

  private findBuckets = restartable(findBuckets)
  private findMeasurements = restartable(findMeasurements)
  private findFields = restartable(findFields)

  public componentDidMount() {
    this.findAndSetBuckets()
  }

  public componentDidUpdate(prevProps: Props) {
    if (prevProps.queryURL !== this.props.queryURL) {
      this.findAndSetBuckets()
    }
  }

  public render() {
    const {
      buckets,
      bucketsStatus,
      bucketsSelection,
      measurements,
      measurementsStatus,
      measurementsSelection,
      fields,
      fieldsStatus,
      fieldsSelection,
      functions,
      functionsSelection,
      functionsStatus,
    } = this.state

    return (
      <div className="query-builder">
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">Select a Bucket</div>
          <BuilderCard
            status={bucketsStatus}
            items={buckets}
            selectedItems={bucketsSelection}
            onSelectItems={this.handleSelectBuckets}
            onSearch={this.findAndSetBuckets}
            limitCount={LIMIT}
            singleSelect={true}
          />
        </div>
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">Select Measurements</div>
          <BuilderCard
            status={measurementsStatus}
            items={measurements}
            selectedItems={measurementsSelection}
            onSelectItems={this.handleSelectMeasurement}
            onSearch={this.findAndSetMeasurements}
            emptyText={EMPTY_MEASUREMENTS_MESSAGE}
            limitCount={LIMIT}
          />
        </div>
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">
            Select Fields
            <small>Optional</small>
          </div>
          <BuilderCard
            status={fieldsStatus}
            items={fields}
            selectedItems={fieldsSelection}
            onSelectItems={this.handleSelectFields}
            onSearch={this.findAndSetFields}
            emptyText={EMPTY_FIELDS_MESSAGE}
            limitCount={LIMIT}
          />
        </div>
        <div className="query-builder--panel">
          <div className="query-builder--panel-header">
            Select Functions
            <small>Optional</small>
          </div>
          <BuilderCard
            status={functionsStatus}
            items={functions}
            selectedItems={functionsSelection}
            onSelectItems={this.handleSelectFunctions}
            onSearch={this.handleSearchFunctions}
            emptyText={EMPTY_FUNCTIONS_MESSAGE}
            limitCount={LIMIT}
          />
        </div>
      </div>
    )
  }

  private findAndSetBuckets = async (
    searchTerm: string = ''
  ): Promise<void> => {
    const {queryURL, sourceType} = this.props

    this.setState({bucketsStatus: RemoteDataState.Loading})

    try {
      const buckets = await this.findBuckets(queryURL, sourceType, searchTerm)
      const {bucketsSelection} = this.state

      this.setState({
        buckets: mergeUnique(buckets, bucketsSelection),
        bucketsStatus: RemoteDataState.Done,
      })
    } catch (e) {
      if (e instanceof CancellationError) {
        return
      }

      this.setState({bucketsStatus: RemoteDataState.Error})
    }
  }

  private handleSelectBuckets = (bucketsSelection: string[]) => {
    if (bucketsSelection.length) {
      this.setState({bucketsSelection}, () => {
        this.findAndSetMeasurements()
        this.emitConfig()
      })

      return
    }

    this.setState(
      {
        bucketsSelection: [],
        measurements: [],
        measurementsStatus: RemoteDataState.NotStarted,
        measurementsSelection: [],
        fields: [],
        fieldsStatus: RemoteDataState.NotStarted,
        fieldsSelection: [],
        functionsStatus: RemoteDataState.NotStarted,
        functionsSelection: [],
      },
      this.emitConfig
    )
  }

  private findAndSetMeasurements = async (
    searchTerm: string = ''
  ): Promise<void> => {
    const {queryURL, sourceType} = this.props
    const [selectedBucket] = this.state.bucketsSelection

    this.setState({measurementsStatus: RemoteDataState.Loading})

    try {
      const measurements = await this.findMeasurements(
        queryURL,
        sourceType,
        selectedBucket,
        searchTerm
      )

      const {measurementsSelection} = this.state

      this.setState({
        measurements: mergeUnique(measurements, measurementsSelection),
        measurementsStatus: RemoteDataState.Done,
      })
    } catch (e) {
      if (e instanceof CancellationError) {
        return
      }

      this.setState({measurementsStatus: RemoteDataState.Error})
    }
  }

  private handleSelectMeasurement = (measurementsSelection: string[]) => {
    if (measurementsSelection.length) {
      this.setState(
        {
          measurementsSelection,
          functionsStatus: RemoteDataState.Done,
        },
        () => {
          this.findAndSetFields()
          this.emitConfig()
        }
      )

      return
    }

    this.setState(
      {
        measurementsSelection: [],
        fields: [],
        fieldsStatus: RemoteDataState.NotStarted,
        fieldsSelection: [],
        functionsStatus: RemoteDataState.NotStarted,
        functionsSelection: [],
      },
      this.emitConfig
    )
  }

  private findAndSetFields = async (searchTerm: string = ''): Promise<void> => {
    const {queryURL, sourceType} = this.props
    const {measurementsSelection} = this.state
    const [selectedBucket] = this.state.bucketsSelection

    this.setState({fieldsStatus: RemoteDataState.Loading})

    try {
      const fields = await this.findFields(
        queryURL,
        sourceType,
        selectedBucket,
        measurementsSelection,
        searchTerm
      )

      const {fieldsSelection} = this.state

      this.setState({
        fields: mergeUnique(fields, fieldsSelection),
        fieldsStatus: RemoteDataState.Done,
      })
    } catch (e) {
      if (e instanceof CancellationError) {
        return
      }

      this.setState({fieldsStatus: RemoteDataState.Error})
    }
  }

  private handleSelectFields = (fieldsSelection: string[]) => {
    this.setState({fieldsSelection}, this.emitConfig)
  }

  private handleSelectFunctions = (functionsSelection: string[]) => {
    this.setState({functionsSelection}, this.emitConfig)
  }

  private handleSearchFunctions = async (searchTerm: string) => {
    const {functionsSelection} = this.state

    const functions = FUNCTIONS.map(f => f.name).filter(name =>
      name.toLowerCase().includes(searchTerm.toLowerCase())
    )

    this.setState({functions: mergeUnique(functions, functionsSelection)})
  }

  private emitConfig = (): void => {
    const {onBuildQuery} = this.props
    const {
      bucketsSelection,
      measurementsSelection,
      fieldsSelection,
      functionsSelection,
    } = this.state

    const config: BuilderConfig = {
      buckets: bucketsSelection,
      measurements: measurementsSelection,
      fields: fieldsSelection,
      functions: functionsSelection,
    }

    onBuildQuery(config)
  }
}

const mstp = (state: AppState): StateProps => {
  const sources = getSources(state)
  const activeSource = getActiveSource(state)
  const activeQuery = getActiveQuery(state)

  let source: Source

  if (activeQuery.sourceID) {
    source = sources.find(source => source.id === activeQuery.sourceID)
  } else {
    source = activeSource
  }

  const queryURL = source.links.query
  const sourceType = source.type

  return {queryURL, sourceType}
}

const mdtp = {
  onBuildQuery: buildQuery,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueryBuilder)
