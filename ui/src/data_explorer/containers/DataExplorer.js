import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'
import queryString from 'query-string'

import _ from 'lodash'

import QueryMaker from '../components/QueryMaker'
import Visualization from '../components/Visualization'
import WriteDataForm from 'src/data_explorer/components/WriteDataForm'
import Header from '../containers/Header'
import ResizeContainer from 'shared/components/ResizeContainer'
import OverlayTechnologies from 'shared/components/OverlayTechnologies'
import ManualRefresh from 'src/shared/components/ManualRefresh'

import {VIS_VIEWS, AUTO_GROUP_BY} from 'shared/constants'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from '../constants'
import {errorThrown} from 'shared/actions/errors'
import {setAutoRefresh} from 'shared/actions/app'
import * as dataExplorerActionCreators from 'src/data_explorer/actions/view'
import {writeLineProtocolAsync} from 'src/data_explorer/actions/view/write'
import {buildRawText} from 'src/utils/influxql'

class DataExplorer extends Component {
  constructor(props) {
    super(props)

    this.state = {
      showWriteForm: false,
    }
  }

  getActiveQuery = () => {
    const {queryConfigs} = this.props
    if (queryConfigs.length === 0) {
      this.props.queryConfigActions.addQuery()
    }

    return queryConfigs[0]
  }

  componentDidMount() {
    const {router} = this.props
    const {query} = queryString.parse(router.location.search)
    if (query && query.length) {
      const qc = this.props.queryConfigs[0]
      this.props.queryConfigActions.editRawText(qc.id, query)
    }
  }

  componentWillReceiveProps(nextProps) {
    const {router} = this.props
    const {queryConfigs, timeRange} = nextProps
    const query = buildRawText(_.get(queryConfigs, ['0'], ''), timeRange)
    const qsCurrent = queryString.parse(router.location.search)
    if (query.length && qsCurrent.query !== query) {
      const qsNew = queryString.stringify({query})
      router.push(`${router.location.pathname}?${qsNew}`)
    }
  }

  handleCloseWriteData = () => {
    this.setState({showWriteForm: false})
  }

  handleOpenWriteData = () => {
    this.setState({showWriteForm: true})
  }

  handleChooseTimeRange = bounds => {
    this.props.setTimeRange(bounds)
  }

  render() {
    const {
      source,
      timeRange,
      autoRefresh,
      queryConfigs,
      manualRefresh,
      onManualRefresh,
      errorThrownAction,
      writeLineProtocol,
      queryConfigActions,
      handleChooseAutoRefresh,
    } = this.props

    const {showWriteForm} = this.state
    const selectedDatabase = _.get(queryConfigs, ['0', 'database'], null)
    const interval = {
      id: 'interval',
      type: 'autoGroupBy',
      tempVar: ':interval:',
      label: 'automatically determine the best group by time',
      values: [
        {
          value: '1000', // pixels
          type: 'resolution',
          selected: true,
        },
        {
          value: '3',
          type: 'pointsPerPixel',
          selected: true,
        },
      ],
    }

    const templates = [interval]

    return (
      <div className="data-explorer">
        {showWriteForm
          ? <OverlayTechnologies>
              <WriteDataForm
                source={source}
                errorThrown={errorThrownAction}
                selectedDatabase={selectedDatabase}
                onClose={this.handleCloseWriteData}
                writeLineProtocol={writeLineProtocol}
              />
            </OverlayTechnologies>
          : null}
        <Header
          timeRange={timeRange}
          autoRefresh={autoRefresh}
          showWriteForm={this.handleOpenWriteData}
          onChooseTimeRange={this.handleChooseTimeRange}
          onChooseAutoRefresh={handleChooseAutoRefresh}
          onManualRefresh={onManualRefresh}
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
            actions={queryConfigActions}
            timeRange={timeRange}
            activeQuery={this.getActiveQuery()}
            initialGroupByTime={AUTO_GROUP_BY}
          />
          <Visualization
            views={VIS_VIEWS}
            activeQueryIndex={0}
            timeRange={timeRange}
            templates={templates}
            autoRefresh={autoRefresh}
            queryConfigs={queryConfigs}
            manualRefresh={manualRefresh}
            errorThrown={errorThrownAction}
            editQueryStatus={queryConfigActions.editQueryStatus}
          />
        </ResizeContainer>
      </div>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

DataExplorer.propTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
      queries: string.isRequired,
    }).isRequired,
  }).isRequired,
  router: shape({
    location: shape({
      search: string,
      pathanme: string,
    }),
  }),
  queryConfigs: arrayOf(shape({})).isRequired,
  queryConfigActions: shape({
    editQueryStatus: func.isRequired,
  }).isRequired,
  autoRefresh: number.isRequired,
  handleChooseAutoRefresh: func.isRequired,
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  setTimeRange: func.isRequired,
  dataExplorer: shape({
    queryIDs: arrayOf(string).isRequired,
  }).isRequired,
  writeLineProtocol: func.isRequired,
  errorThrownAction: func.isRequired,
  onManualRefresh: func.isRequired,
  manualRefresh: number.isRequired,
}

DataExplorer.childContextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
    }).isRequired,
  }).isRequired,
}

const mapStateToProps = state => {
  const {
    app: {persisted: {autoRefresh}},
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
