import React, {Component, PropTypes} from 'react'

import _ from 'lodash'
import uuid from 'node-uuid'

import ResizeContainer from 'shared/components/ResizeContainer'
import QueryMaker from 'src/dashboards/components/QueryMaker'
import Visualization from 'src/dashboards/components/Visualization'
import OverlayControls from 'src/dashboards/components/OverlayControls'
import DisplayOptions from 'src/dashboards/components/DisplayOptions'

import * as queryModifiers from 'src/utils/queryTransitions'

import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import buildInfluxQLQuery from 'utils/influxql'
import {getQueryConfig} from 'shared/apis'

import {removeUnselectedTemplateValues} from 'src/dashboards/constants'
import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from 'src/data_explorer/constants'

class CellEditorOverlay extends Component {
  constructor(props) {
    super(props)

    const {cell: {name, type, queries, axes}} = props

    const queriesWorkingDraft = _.cloneDeep(
      queries.map(({queryConfig}) => ({...queryConfig, id: uuid.v4()}))
    )

    this.state = {
      cellWorkingName: name,
      cellWorkingType: type,
      queriesWorkingDraft,
      activeQueryIndex: 0,
      isDisplayOptionsTabActive: false,
      axes,
    }
  }

  componentWillReceiveProps(nextProps) {
    const {status, queryID} = this.props.queryStatus
    const nextStatus = nextProps.queryStatus
    if (nextStatus.status && nextStatus.queryID) {
      if (nextStatus.queryID !== queryID || nextStatus.status !== status) {
        const nextQueries = this.state.queriesWorkingDraft.map(
          q => (q.id === queryID ? {...q, status: nextStatus.status} : q)
        )
        this.setState({queriesWorkingDraft: nextQueries})
      }
    }
  }

  queryStateReducer = queryModifier => (queryID, payload) => {
    const {queriesWorkingDraft} = this.state
    const query = queriesWorkingDraft.find(q => q.id === queryID)

    const nextQuery = queryModifier(query, payload)

    const nextQueries = queriesWorkingDraft.map(
      q => (q.id === query.id ? nextQuery : q)
    )
    this.setState({queriesWorkingDraft: nextQueries})
  }

  handleSetYAxisBoundMin = min => {
    const {axes} = this.state
    const {y: {bounds: [, max]}} = axes

    this.setState({
      axes: {...axes, y: {...axes.y, bounds: [min, max]}},
    })
  }

  handleSetYAxisBoundMax = max => {
    const {axes} = this.state
    const {y: {bounds: [min]}} = axes

    this.setState({
      axes: {...axes, y: {...axes.y, bounds: [min, max]}},
    })
  }

  handleSetLabel = label => {
    const {axes} = this.state

    this.setState({axes: {...axes, y: {...axes.y, label}}})
  }

  handleSetPrefixSuffix = e => {
    const {axes} = this.state
    const {prefix, suffix} = e.target.form

    this.setState({
      axes: {
        ...axes,
        y: {
          ...axes.y,
          prefix: prefix.value,
          suffix: suffix.value,
        },
      },
    })
  }

  handleAddQuery = () => {
    const {queriesWorkingDraft} = this.state
    const newIndex = queriesWorkingDraft.length

    this.setState({
      queriesWorkingDraft: [
        ...queriesWorkingDraft,
        defaultQueryConfig({id: uuid.v4()}),
      ],
    })
    this.handleSetActiveQueryIndex(newIndex)
  }

  handleDeleteQuery = index => {
    const nextQueries = this.state.queriesWorkingDraft.filter(
      (__, i) => i !== index
    )
    this.setState({queriesWorkingDraft: nextQueries})
  }

  handleSaveCell = () => {
    const {
      queriesWorkingDraft,
      cellWorkingType: type,
      cellWorkingName: name,
      axes,
    } = this.state

    const {cell} = this.props

    const queries = queriesWorkingDraft.map(q => {
      const timeRange = q.range || {upper: null, lower: ':dashboardTime:'}
      const query = q.rawText || buildInfluxQLQuery(timeRange, q)

      return {
        queryConfig: q,
        query,
      }
    })

    this.props.onSave({
      ...cell,
      name,
      type,
      queries,
      axes,
    })
  }

  handleSelectGraphType = graphType => () => {
    this.setState({cellWorkingType: graphType})
  }

  handleClickDisplayOptionsTab = isDisplayOptionsTabActive => () => {
    this.setState({isDisplayOptionsTabActive})
  }

  handleSetActiveQueryIndex = activeQueryIndex => {
    this.setState({activeQueryIndex})
  }

  handleSetBase = base => () => {
    const {axes} = this.state

    this.setState({
      axes: {
        ...axes,
        y: {
          ...axes.y,
          base,
        },
      },
    })
  }

  handleCellRename = newName => {
    this.setState({cellWorkingName: newName})
  }

  handleSetScale = scale => () => {
    const {axes} = this.state

    this.setState({
      axes: {
        ...axes,
        y: {
          ...axes.y,
          scale,
        },
      },
    })
  }

  getActiveQuery = () => {
    const {queriesWorkingDraft, activeQueryIndex} = this.state
    const activeQuery = queriesWorkingDraft[activeQueryIndex]
    const defaultQuery = queriesWorkingDraft[0]

    return activeQuery || defaultQuery
  }

  handleEditRawText = async (url, id, text) => {
    const templates = removeUnselectedTemplateValues(this.props.templates)

    // use this as the handler passed into fetchTimeSeries to update a query status
    try {
      const {data} = await getQueryConfig(url, [{query: text, id}], templates)
      const config = data.queries.find(q => q.id === id)
      const nextQueries = this.state.queriesWorkingDraft.map(
        q => (q.id === id ? config.queryConfig : q)
      )
      this.setState({queriesWorkingDraft: nextQueries})
    } catch (error) {
      console.error(error)
    }
  }

  render() {
    const {
      source,
      onCancel,
      templates,
      timeRange,
      autoRefresh,
      editQueryStatus,
    } = this.props

    const {
      activeQueryIndex,
      cellWorkingName,
      cellWorkingType,
      isDisplayOptionsTabActive,
      queriesWorkingDraft,
      axes,
    } = this.state

    const queryActions = {
      editRawTextAsync: this.handleEditRawText,
      ..._.mapValues(queryModifiers, qm => this.queryStateReducer(qm)),
    }

    const isQuerySavable = query =>
      (!!query.measurement && !!query.database && !!query.fields.length) ||
      !!query.rawText

    return (
      <div className={OVERLAY_TECHNOLOGY}>
        <ResizeContainer
          containerClass="resizer--full-size"
          minTopHeight={MINIMUM_HEIGHTS.visualization}
          minBottomHeight={MINIMUM_HEIGHTS.queryMaker}
          initialTopHeight={INITIAL_HEIGHTS.visualization}
          initialBottomHeight={INITIAL_HEIGHTS.queryMaker}
        >
          <Visualization
            axes={axes}
            type={cellWorkingType}
            name={cellWorkingName}
            timeRange={timeRange}
            templates={templates}
            autoRefresh={autoRefresh}
            queryConfigs={queriesWorkingDraft}
            editQueryStatus={editQueryStatus}
            onCellRename={this.handleCellRename}
          />
          <CEOBottom>
            <OverlayControls
              isDisplayOptionsTabActive={isDisplayOptionsTabActive}
              onClickDisplayOptions={this.handleClickDisplayOptionsTab}
              onCancel={onCancel}
              onSave={this.handleSaveCell}
              isSavable={queriesWorkingDraft.every(isQuerySavable)}
            />
            {isDisplayOptionsTabActive
              ? <DisplayOptions
                  axes={axes}
                  onSetBase={this.handleSetBase}
                  onSetLabel={this.handleSetLabel}
                  onSetScale={this.handleSetScale}
                  queryConfigs={queriesWorkingDraft}
                  selectedGraphType={cellWorkingType}
                  onSetPrefixSuffix={this.handleSetPrefixSuffix}
                  onSelectGraphType={this.handleSelectGraphType}
                  onSetYAxisBoundMin={this.handleSetYAxisBoundMin}
                  onSetYAxisBoundMax={this.handleSetYAxisBoundMax}
                />
              : <QueryMaker
                  source={source}
                  templates={templates}
                  queries={queriesWorkingDraft}
                  actions={queryActions}
                  autoRefresh={autoRefresh}
                  timeRange={timeRange}
                  onDeleteQuery={this.handleDeleteQuery}
                  onAddQuery={this.handleAddQuery}
                  activeQueryIndex={activeQueryIndex}
                  activeQuery={this.getActiveQuery()}
                  setActiveQueryIndex={this.handleSetActiveQueryIndex}
                />}
          </CEOBottom>
        </ResizeContainer>
      </div>
    )
  }
}

const CEOBottom = ({children}) =>
  <div className="overlay-technology--editor">
    {children}
  </div>

const {arrayOf, func, node, number, shape, string} = PropTypes

CellEditorOverlay.propTypes = {
  onCancel: func.isRequired,
  onSave: func.isRequired,
  cell: shape({}).isRequired,
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ).isRequired,
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  autoRefresh: number.isRequired,
  source: shape({
    links: shape({
      proxy: string.isRequired,
      queries: string.isRequired,
    }).isRequired,
  }).isRequired,
  editQueryStatus: func.isRequired,
  queryStatus: shape({
    queryID: string,
    status: shape({}),
  }).isRequired,
  dashboardID: string.isRequired,
}

CEOBottom.propTypes = {
  children: node,
}

export default CellEditorOverlay
