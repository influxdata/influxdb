import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter, InjectedRouter} from 'react-router'
import {Location} from 'history'
import queryString from 'query-string'

import _ from 'lodash'

import {stripPrefix} from 'src/utils/basepath'

import QueryMaker from 'src/data_explorer/components/QueryMaker'
import Visualization from 'src/data_explorer/components/Visualization'
import WriteDataForm from 'src/data_explorer/components/WriteDataForm'
import ResizeContainer from 'src/shared/components/ResizeContainer'
import OverlayTechnologies from 'src/shared/components/OverlayTechnologies'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import AutoRefreshDropdown from 'src/shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/GraphTips'
import PageHeader from 'src/shared/components/PageHeader'

import {VIS_VIEWS, AUTO_GROUP_BY, TEMPLATES} from 'src/shared/constants'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from 'src/data_explorer/constants'
import {errorThrown} from 'src/shared/actions/errors'
import {setAutoRefresh} from 'src/shared/actions/app'
import * as dataExplorerActionCreators from 'src/data_explorer/actions/view'
import {writeLineProtocolAsync} from 'src/data_explorer/actions/view/write'
import {buildRawText} from 'src/utils/influxql'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'

import {Source, QueryConfig, TimeRange} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  queryConfigs: QueryConfig[]
  queryConfigActions: any
  autoRefresh: number
  handleChooseAutoRefresh: () => void
  router?: InjectedRouter
  location?: Location
  setTimeRange: (range: TimeRange) => void
  timeRange: TimeRange
  manualRefresh: number
  onManualRefresh: () => void
  errorThrownAction: () => void
  writeLineProtocol: () => void
}

interface State {
  showWriteForm: boolean
}

@ErrorHandling
export class DataExplorer extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      showWriteForm: false,
    }
  }

  public componentDidMount() {
    const {source} = this.props
    const {query} = queryString.parse(location.search)
    if (query && query.length) {
      const qc = this.props.queryConfigs[0]
      this.props.queryConfigActions.editRawTextAsync(
        source.links.queries,
        qc.id,
        query
      )
    }
  }

  public componentWillReceiveProps(nextProps: Props) {
    const {router} = this.props
    const {queryConfigs, timeRange} = nextProps

    const query = buildRawText(_.get(queryConfigs, ['0'], ''), timeRange)
    const qsCurrent = queryString.parse(location.search)

    if (query.length && qsCurrent.query !== query) {
      const qsNew = queryString.stringify({query})
      const pathname = stripPrefix(location.pathname)
      router.push(`${pathname}?${qsNew}`)
    }
  }

  public render() {
    const {
      source,
      timeRange,
      autoRefresh,
      queryConfigs,
      manualRefresh,
      errorThrownAction,
      writeLineProtocol,
      queryConfigActions,
    } = this.props

    const {showWriteForm} = this.state

    return (
      <>
        {showWriteForm ? (
          <OverlayTechnologies>
            <WriteDataForm
              source={source}
              errorThrown={errorThrownAction}
              selectedDatabase={this.selectedDatabase}
              onClose={this.handleCloseWriteData}
              writeLineProtocol={writeLineProtocol}
            />
          </OverlayTechnologies>
        ) : null}
        <PageHeader
          titleText="Data Explorer"
          fullWidth={true}
          renderPageControls={this.renderPageControls}
          sourceIndicator={true}
        />
        <ResizeContainer
          containerClass="page-contents"
          minTopHeight={MINIMUM_HEIGHTS.queryMaker}
          minBottomHeight={MINIMUM_HEIGHTS.visualization}
          initialTopHeight={INITIAL_HEIGHTS.queryMaker}
          initialBottomHeight={INITIAL_HEIGHTS.visualization}
        >
          <QueryMaker
            source={source}
            rawText={this.rawText}
            actions={queryConfigActions}
            activeQuery={this.activeQuery}
            initialGroupByTime={AUTO_GROUP_BY}
          />
          <Visualization
            source={source}
            views={VIS_VIEWS}
            activeQueryIndex={0}
            timeRange={timeRange}
            templates={TEMPLATES}
            autoRefresh={autoRefresh}
            queryConfigs={queryConfigs}
            manualRefresh={manualRefresh}
            errorThrown={errorThrownAction}
            editQueryStatus={queryConfigActions.editQueryStatus}
          />
        </ResizeContainer>
      </>
    )
  }

  private handleCloseWriteData = (): void => {
    this.setState({showWriteForm: false})
  }

  private handleOpenWriteData = (): void => {
    this.setState({showWriteForm: true})
  }

  private handleChooseTimeRange = (timeRange: TimeRange): void => {
    this.props.setTimeRange(timeRange)
  }

  private get selectedDatabase(): string {
    return _.get(this.props.queryConfigs, ['0', 'database'], null)
  }

  private get activeQuery(): QueryConfig {
    const {queryConfigs} = this.props

    if (queryConfigs.length === 0) {
      const qc = defaultQueryConfig()
      this.props.queryConfigActions.addQuery(qc.id)
      queryConfigs.push(qc)
    }

    return queryConfigs[0]
  }

  get rawText(): string {
    const {timeRange} = this.props
    return buildRawText(this.activeQuery, timeRange)
  }

  private get renderPageControls(): JSX.Element {
    const {
      timeRange,
      autoRefresh,
      onManualRefresh,
      handleChooseAutoRefresh,
    } = this.props

    return (
      <>
        <GraphTips />
        <div
          className="btn btn-sm btn-default"
          onClick={this.handleOpenWriteData}
          data-test="write-data-button"
        >
          <span className="icon pencil" />
          Write Data
        </div>
        <AutoRefreshDropdown
          iconName="refresh"
          selected={autoRefresh}
          onChoose={handleChooseAutoRefresh}
          onManualRefresh={onManualRefresh}
        />
        <TimeRangeDropdown
          selected={timeRange}
          page="DataExplorer"
          onChooseTimeRange={this.handleChooseTimeRange}
        />
      </>
    )
  }
}

const mapStateToProps = state => {
  const {
    app: {
      persisted: {autoRefresh},
    },
    dataExplorer,
    dataExplorerQueryConfigs: queryConfigs,
    timeRange,
  } = state
  const queryConfigValues = _.values(queryConfigs)

  return {
    autoRefresh,
    dataExplorer,
    queryConfigs: queryConfigValues,
    timeRange,
  }
}

const mapDispatchToProps = dispatch => {
  return {
    handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
    errorThrownAction: bindActionCreators(errorThrown, dispatch),
    setTimeRange: bindActionCreators(
      dataExplorerActionCreators.setTimeRange,
      dispatch
    ),
    writeLineProtocol: bindActionCreators(writeLineProtocolAsync, dispatch),
    queryConfigActions: bindActionCreators(
      dataExplorerActionCreators,
      dispatch
    ),
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(ManualRefresh(DataExplorer))
)
