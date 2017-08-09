import React, {Component, PropTypes} from 'react'

import _ from 'lodash'
import uuid from 'node-uuid'

import ResizeContainer from 'shared/components/ResizeContainer'
import QueryMaker from 'src/data_explorer/components/QueryMaker'
import Visualization from 'src/dashboards/components/Visualization'
import OverlayControls from 'src/dashboards/components/OverlayControls'
import DisplayOptions from 'src/dashboards/components/DisplayOptions'

import * as queryModifiers from 'src/utils/queryTransitions'

import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import buildInfluxQLQuery from 'utils/influxql'
import {getQueryConfig} from 'shared/apis'

import {buildYLabel} from 'shared/presenters'
import {removeUnselectedTemplateValues} from 'src/dashboards/constants'
import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from 'src/data_explorer/constants'

class CellEditorOverlay extends Component {
  constructor(props) {
    super(props)

    this.queryStateReducer = ::this.queryStateReducer
    this.handleAddQuery = ::this.handleAddQuery
    this.handleDeleteQuery = ::this.handleDeleteQuery
    this.handleSaveCell = ::this.handleSaveCell
    this.handleSelectGraphType = ::this.handleSelectGraphType
    this.handleClickDisplayOptionsTab = ::this.handleClickDisplayOptionsTab
    this.handleSetActiveQueryIndex = ::this.handleSetActiveQueryIndex
    this.handleEditRawText = ::this.handleEditRawText
    this.handleSetYAxisBounds = ::this.handleSetYAxisBounds
    this.handleSetLabel = ::this.handleSetLabel

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
      axes: this.setDefaultLabels(axes, queries),
    }
  }

  setDefaultLabels(axes, queries) {
    if (!queries.length) {
      return axes
    }

    if (axes.y.label) {
      return axes
    }

    const q = queries[0].queryConfig
    const label = buildYLabel(q)

    return {...axes, y: {...axes.y, label}}
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

  queryStateReducer(queryModifier) {
    return (queryID, payload) => {
      const {queriesWorkingDraft} = this.state
      const query = queriesWorkingDraft.find(q => q.id === queryID)

      const nextQuery = queryModifier(query, payload)

      const nextQueries = queriesWorkingDraft.map(
        q => (q.id === query.id ? nextQuery : q)
      )
      this.setState({queriesWorkingDraft: nextQueries})
    }
  }

  handleSetYAxisBounds(e) {
    const {min, max} = e.target.form
    const {axes} = this.state

    this.setState({
      axes: {...axes, y: {...axes.y, bounds: [min.value, max.value]}},
    })
    e.preventDefault()
  }

  handleSetLabel(e) {
    const {label} = e.target.form
    const {axes} = this.state

    this.setState({axes: {...axes, y: {...axes.y, label: label.value}}})
    e.preventDefault()
  }

  handleAddQuery(options) {
    const newQuery = Object.assign({}, defaultQueryConfig(uuid.v4()), options)
    const nextQueries = this.state.queriesWorkingDraft.concat(newQuery)
    this.setState({queriesWorkingDraft: nextQueries})
  }

  handleDeleteQuery(index) {
    const nextQueries = this.state.queriesWorkingDraft.filter(
      (__, i) => i !== index
    )
    this.setState({queriesWorkingDraft: nextQueries})
  }

  handleSaveCell() {
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

  handleSelectGraphType(graphType) {
    this.setState({cellWorkingType: graphType})
  }

  handleClickDisplayOptionsTab(isDisplayOptionsTabActive) {
    return () => {
      this.setState({isDisplayOptionsTabActive})
    }
  }

  handleSetActiveQueryIndex(activeQueryIndex) {
    this.setState({activeQueryIndex})
  }

  async handleEditRawText(url, id, text) {
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
      addQuery: this.handleAddQuery,
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
            autoRefresh={autoRefresh}
            timeRange={timeRange}
            templates={templates}
            queryConfigs={queriesWorkingDraft}
            activeQueryIndex={0}
            cellType={cellWorkingType}
            cellName={cellWorkingName}
            editQueryStatus={editQueryStatus}
            axes={axes}
            views={[]}
          />
          <div className="overlay-technology--editor">
            <OverlayControls
              isDisplayOptionsTabActive={isDisplayOptionsTabActive}
              onClickDisplayOptions={this.handleClickDisplayOptionsTab}
              onCancel={onCancel}
              onSave={this.handleSaveCell}
              isSavable={queriesWorkingDraft.every(isQuerySavable)}
            />
            {isDisplayOptionsTabActive
              ? <DisplayOptions
                  selectedGraphType={cellWorkingType}
                  onSelectGraphType={this.handleSelectGraphType}
                  onSetRange={this.handleSetYAxisBounds}
                  onSetLabel={this.handleSetLabel}
                  axes={axes}
                />
              : <QueryMaker
                  source={source}
                  templates={templates}
                  queries={queriesWorkingDraft}
                  actions={queryActions}
                  autoRefresh={autoRefresh}
                  timeRange={timeRange}
                  setActiveQueryIndex={this.handleSetActiveQueryIndex}
                  onDeleteQuery={this.handleDeleteQuery}
                  activeQueryIndex={activeQueryIndex}
                />}
          </div>
        </ResizeContainer>
      </div>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

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

export default CellEditorOverlay
