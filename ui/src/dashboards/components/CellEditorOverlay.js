import React, {Component, PropTypes} from 'react'

import _ from 'lodash'
import uuid from 'node-uuid'

import ResizeContainer, {ResizeBottom} from 'src/shared/components/ResizeContainer'
import QueryBuilder from 'src/data_explorer/components/QueryBuilder'
import Visualization from 'src/data_explorer/components/Visualization'
import OverlayControls from 'src/dashboards/components/OverlayControls'
import * as queryModifiers from 'src/utils/queryTransitions'

import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import buildInfluxQLQuery from 'utils/influxql'

class CellEditorOverlay extends Component {
  constructor(props) {
    super(props)

    this.queryStateReducer = ::this.queryStateReducer

    this.handleAddQuery = ::this.handleAddQuery
    this.handleDeleteQuery = ::this.handleDeleteQuery

    this.handleSaveCell = ::this.handleSaveCell

    this.handleSelectGraphType = ::this.handleSelectGraphType
    this.handleSetActiveQueryIndex = ::this.handleSetActiveQueryIndex

    const {cell: {name, type, queries}} = props
    const queriesWorkingDraft = _.cloneDeep(queries.map(({queryConfig}) => queryConfig))

    this.state = {
      cellWorkingName: name,
      cellWorkingType: type,
      queriesWorkingDraft,
      activeQueryIndex: 0,
    }
  }

  queryStateReducer(queryModifier) {
    return (queryID, payload) => {
      const {queriesWorkingDraft} = this.state
      const query = queriesWorkingDraft.find((q) => q.id === queryID)

      const nextQuery = queryModifier(query, payload)

      const nextQueries = queriesWorkingDraft.map((q) => q.id === query.id ? nextQuery : q)
      this.setState({queriesWorkingDraft: nextQueries})
    }
  }

  handleAddQuery(options) {
    const newQuery = Object.assign({}, defaultQueryConfig(uuid.v4()), options)
    const nextQueries = this.state.queriesWorkingDraft.concat(newQuery)
    this.setState({queriesWorkingDraft: nextQueries})
  }

  handleDeleteQuery(index) {
    const nextQueries = this.state.queriesWorkingDraft.filter((__, i) => i !== index)
    this.setState({queriesWorkingDraft: nextQueries})
  }

  handleSaveCell() {
    const {queriesWorkingDraft, cellWorkingType, cellWorkingName} = this.state
    const {cell, timeRange} = this.props

    const newCell = _.cloneDeep(cell)
    newCell.name = cellWorkingName
    newCell.type = cellWorkingType
    newCell.queries = queriesWorkingDraft.map((q) => {
      const query = q.rawText || buildInfluxQLQuery(timeRange, q)
      const label = `${q.measurement}.${q.fields[0].field}`

      return {
        queryConfig: q,
        query,
        label,
      }
    })

    this.props.onSave(newCell)
  }

  handleSelectGraphType(graphType) {
    this.setState({cellWorkingType: graphType})
  }

  handleSetActiveQueryIndex(activeQueryIndex) {
    this.setState({activeQueryIndex})
  }

  render() {
    const {onCancel, autoRefresh, timeRange} = this.props
    const {
      activeQueryIndex,
      cellWorkingName,
      cellWorkingType,
      queriesWorkingDraft,
    } = this.state

    const queryActions = {
      addQuery: this.handleAddQuery,
      ..._.mapValues(queryModifiers, (qm) => this.queryStateReducer(qm)),
    }

    return (
      <div className="data-explorer overlay-technology">
        <ResizeContainer>
          <Visualization
            autoRefresh={autoRefresh}
            timeRange={timeRange}
            queryConfigs={queriesWorkingDraft}
            activeQueryIndex={0}
            cellType={cellWorkingType}
            cellName={cellWorkingName}
          />
          <ResizeBottom>
            <OverlayControls
              selectedGraphType={cellWorkingType}
              onSelectGraphType={this.handleSelectGraphType}
              onCancel={onCancel}
              onSave={this.handleSaveCell}
            />
            <QueryBuilder
              queries={queriesWorkingDraft}
              actions={queryActions}
              autoRefresh={autoRefresh}
              timeRange={timeRange}
              setActiveQueryIndex={this.handleSetActiveQueryIndex}
              onDeleteQuery={this.handleDeleteQuery}
              activeQueryIndex={activeQueryIndex}
            />
          </ResizeBottom>
        </ResizeContainer>
      </div>
    )
  }
}

const {
  func,
  number,
  shape,
  string,
} = PropTypes

CellEditorOverlay.propTypes = {
  onCancel: func.isRequired,
  onSave: func.isRequired,
  cell: shape({}).isRequired,
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  autoRefresh: number.isRequired,
}

export default CellEditorOverlay
