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
import {buildQuery} from 'utils/influxql'
import {getQueryConfig} from 'shared/apis'
import {GET_STATIC_LEGEND} from 'src/shared/constants'

import {
  removeUnselectedTemplateValues,
  TYPE_QUERY_CONFIG,
} from 'src/dashboards/constants'
import {OVERLAY_TECHNOLOGY} from 'shared/constants/classNames'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from 'src/data_explorer/constants'
import {AUTO_GROUP_BY} from 'shared/constants'
import {
  COLOR_TYPE_THRESHOLD,
  MAX_THRESHOLDS,
  DEFAULT_VALUE_MIN,
  DEFAULT_VALUE_MAX,
  GAUGE_COLORS,
  SINGLE_STAT_TEXT,
  SINGLE_STAT_BG,
  validateColors,
} from 'src/dashboards/constants/gaugeColors'

class CellEditorOverlay extends Component {
  constructor(props) {
    super(props)

    const {cell: {name, type, queries, axes, colors, legend}, sources} = props

    let source = _.get(queries, ['0', 'source'], null)
    source = sources.find(s => s.links.self === source) || props.source

    const queriesWorkingDraft = _.cloneDeep(
      queries.map(({queryConfig}) => ({
        ...queryConfig,
        id: uuid.v4(),
        source,
      }))
    )
    const colorsTypeContainsText = _.some(colors, {type: SINGLE_STAT_TEXT})

    this.state = {
      cellWorkingName: name,
      cellWorkingType: type,
      queriesWorkingDraft,
      activeQueryIndex: 0,
      isDisplayOptionsTabActive: false,
      axes,
      colorSingleStatText: colorsTypeContainsText,
      colors: validateColors(colors, type, colorsTypeContainsText),
      staticLegend: GET_STATIC_LEGEND(legend),
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

  componentDidMount = () => {
    this.overlayRef.focus()
  }

  handleAddThreshold = () => {
    const {colors, cellWorkingType} = this.state
    const sortedColors = _.sortBy(colors, color => Number(color.value))

    if (sortedColors.length <= MAX_THRESHOLDS) {
      const randomColor = _.random(0, GAUGE_COLORS.length - 1)

      const maxValue =
        cellWorkingType === 'gauge'
          ? Number(sortedColors[sortedColors.length - 1].value)
          : DEFAULT_VALUE_MAX
      const minValue =
        cellWorkingType === 'gauge'
          ? Number(sortedColors[0].value)
          : DEFAULT_VALUE_MIN

      const colorsValues = _.mapValues(colors, 'value')
      let randomValue

      do {
        randomValue = `${_.round(_.random(minValue, maxValue, true), 2)}`
      } while (_.includes(colorsValues, randomValue))

      const newThreshold = {
        type: COLOR_TYPE_THRESHOLD,
        id: uuid.v4(),
        value: randomValue,
        hex: GAUGE_COLORS[randomColor].hex,
        name: GAUGE_COLORS[randomColor].name,
      }

      this.setState({colors: [...colors, newThreshold]})
    }
  }

  handleDeleteThreshold = threshold => () => {
    const {colors} = this.state

    const newColors = colors.filter(color => color.id !== threshold.id)

    this.setState({colors: newColors})
  }

  handleChooseColor = threshold => chosenColor => {
    const {colors} = this.state

    const newColors = colors.map(
      color =>
        color.id === threshold.id
          ? {...color, hex: chosenColor.hex, name: chosenColor.name}
          : color
    )

    this.setState({colors: newColors})
  }

  handleUpdateColorValue = (threshold, newValue) => {
    const {colors} = this.state
    const newColors = colors.map(
      color => (color.id === threshold.id ? {...color, value: newValue} : color)
    )
    this.setState({colors: newColors})
  }

  handleValidateColorValue = (threshold, e) => {
    const {colors, cellWorkingType} = this.state
    const sortedColors = _.sortBy(colors, color => Number(color.value))
    const thresholdValue = Number(threshold.value)
    const targetValueNumber = Number(e.target.value)
    let allowedToUpdate = false

    if (cellWorkingType === 'single-stat') {
      // If type is single-stat then value only has to be unique
      return !sortedColors.some(color => color.value === e.target.value)
    }

    const minValue = Number(sortedColors[0].value)
    const maxValue = Number(sortedColors[sortedColors.length - 1].value)

    // If lowest value, make sure it is less than the next threshold
    if (thresholdValue === minValue) {
      const nextValue = Number(sortedColors[1].value)
      allowedToUpdate = targetValueNumber < nextValue
    }
    // If highest value, make sure it is greater than the previous threshold
    if (thresholdValue === maxValue) {
      const previousValue = Number(sortedColors[sortedColors.length - 2].value)
      allowedToUpdate = previousValue < targetValueNumber
    }
    // If not min or max, make sure new value is greater than min, less than max, and unique
    if (thresholdValue !== minValue && thresholdValue !== maxValue) {
      const greaterThanMin = targetValueNumber > minValue
      const lessThanMax = targetValueNumber < maxValue

      const colorsWithoutMinOrMax = sortedColors.slice(
        1,
        sortedColors.length - 1
      )

      const isUnique = !colorsWithoutMinOrMax.some(
        color => color.value === e.target.value
      )

      allowedToUpdate = greaterThanMin && lessThanMax && isUnique
    }

    return allowedToUpdate
  }

  handleToggleSingleStatText = () => {
    const {colors, colorSingleStatText} = this.state
    const formattedColors = colors.map(color => ({
      ...color,
      type: colorSingleStatText ? SINGLE_STAT_BG : SINGLE_STAT_TEXT,
    }))

    this.setState({
      colorSingleStatText: !colorSingleStatText,
      colors: formattedColors,
    })
  }

  handleSetSuffix = e => {
    const {axes} = this.state

    this.setState({
      axes: {
        ...axes,
        y: {
          ...axes.y,
          suffix: e.target.value,
        },
      },
    })
  }

  queryStateReducer = queryModifier => (queryID, ...payload) => {
    const {queriesWorkingDraft} = this.state
    const query = queriesWorkingDraft.find(q => q.id === queryID)

    const nextQuery = queryModifier(query, ...payload)

    const nextQueries = queriesWorkingDraft.map(
      q =>
        q.id === query.id
          ? {...nextQuery, source: this.nextSource(q, nextQuery)}
          : q
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
      colors,
      staticLegend,
    } = this.state

    const {cell} = this.props

    const queries = queriesWorkingDraft.map(q => {
      const timeRange = q.range || {upper: null, lower: ':dashboardTime:'}
      const query = q.rawText || buildQuery(TYPE_QUERY_CONFIG, timeRange, q)

      return {
        queryConfig: q,
        query,
        source: _.get(q, ['source', 'links', 'self'], null),
      }
    })

    this.props.onSave({
      ...cell,
      name,
      type,
      queries,
      axes,
      colors,
      legend: staticLegend
        ? {
            type: 'static',
            orientation: 'bottom',
          }
        : {},
    })
  }

  handleSelectGraphType = graphType => () => {
    const {colors, colorSingleStatText} = this.state
    const validatedColors = validateColors(
      colors,
      graphType,
      colorSingleStatText
    )
    this.setState({cellWorkingType: graphType, colors: validatedColors})
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

  handleToggleStaticLegend = staticLegend => () => {
    this.setState({staticLegend})
  }

  handleSetQuerySource = source => {
    const queriesWorkingDraft = this.state.queriesWorkingDraft.map(q => ({
      ..._.cloneDeep(q),
      source,
    }))

    this.setState({queriesWorkingDraft})
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
        q => (q.id === id ? {...config.queryConfig, source: q.source} : q)
      )
      this.setState({queriesWorkingDraft: nextQueries})
    } catch (error) {
      console.error(error)
    }
  }

  formatSources = this.props.sources.map(s => ({
    ...s,
    text: `${s.name} @ ${s.url}`,
  }))

  findSelectedSource = () => {
    const {source} = this.props
    const sources = this.formatSources
    const query = _.get(this.state.queriesWorkingDraft, 0, false)

    if (!query || !query.source) {
      const defaultSource = sources.find(s => s.id === source.id)
      return (defaultSource && defaultSource.text) || 'No sources'
    }

    const selected = sources.find(s => s.id === query.source.id)
    return (selected && selected.text) || 'No sources'
  }

  getSource = () => {
    const {source, sources} = this.props
    const query = _.get(this.state.queriesWorkingDraft, 0, false)

    if (!query || !query.source) {
      return source
    }

    const querySource = sources.find(s => s.id === query.source.id)
    return querySource || source
  }

  nextSource = (prevQuery, nextQuery) => {
    if (nextQuery.source) {
      return nextQuery.source
    }

    return prevQuery.source
  }

  handleKeyDown = e => {
    if (e.key === 'Enter' && e.metaKey && e.target === this.overlayRef) {
      this.handleSaveCell()
    }
    if (e.key === 'Enter' && e.metaKey && e.target !== this.overlayRef) {
      e.target.blur()
      setTimeout(this.handleSaveCell, 50)
    }
    if (e.key === 'Escape' && e.target === this.overlayRef) {
      this.props.onCancel()
    }
    if (e.key === 'Escape' && e.target !== this.overlayRef) {
      e.target.blur()
      this.overlayRef.focus()
    }
  }

  render() {
    const {
      onCancel,
      templates,
      timeRange,
      autoRefresh,
      editQueryStatus,
    } = this.props

    const {
      axes,
      colors,
      activeQueryIndex,
      cellWorkingName,
      cellWorkingType,
      isDisplayOptionsTabActive,
      queriesWorkingDraft,
      colorSingleStatText,
      staticLegend,
    } = this.state

    const queryActions = {
      editRawTextAsync: this.handleEditRawText,
      ..._.mapValues(queryModifiers, qm => this.queryStateReducer(qm)),
    }

    const isQuerySavable = query =>
      (!!query.measurement && !!query.database && !!query.fields.length) ||
      !!query.rawText

    return (
      <div
        className={OVERLAY_TECHNOLOGY}
        onKeyDown={this.handleKeyDown}
        tabIndex="0"
        ref={r => (this.overlayRef = r)}
      >
        <ResizeContainer
          containerClass="resizer--full-size"
          minTopHeight={MINIMUM_HEIGHTS.visualization}
          minBottomHeight={MINIMUM_HEIGHTS.queryMaker}
          initialTopHeight={INITIAL_HEIGHTS.visualization}
          initialBottomHeight={INITIAL_HEIGHTS.queryMaker}
        >
          <Visualization
            axes={axes}
            colors={colors}
            type={cellWorkingType}
            name={cellWorkingName}
            timeRange={timeRange}
            templates={templates}
            autoRefresh={autoRefresh}
            queryConfigs={queriesWorkingDraft}
            editQueryStatus={editQueryStatus}
            onCellRename={this.handleCellRename}
            staticLegend={staticLegend}
          />
          <CEOBottom>
            <OverlayControls
              onCancel={onCancel}
              queries={queriesWorkingDraft}
              sources={this.formatSources}
              onSave={this.handleSaveCell}
              selected={this.findSelectedSource()}
              onSetQuerySource={this.handleSetQuerySource}
              isSavable={queriesWorkingDraft.every(isQuerySavable)}
              isDisplayOptionsTabActive={isDisplayOptionsTabActive}
              onClickDisplayOptions={this.handleClickDisplayOptionsTab}
            />
            {isDisplayOptionsTabActive
              ? <DisplayOptions
                  axes={axes}
                  colors={colors}
                  onChooseColor={this.handleChooseColor}
                  onValidateColorValue={this.handleValidateColorValue}
                  onUpdateColorValue={this.handleUpdateColorValue}
                  onAddThreshold={this.handleAddThreshold}
                  onDeleteThreshold={this.handleDeleteThreshold}
                  onToggleSingleStatText={this.handleToggleSingleStatText}
                  colorSingleStatText={colorSingleStatText}
                  onSetBase={this.handleSetBase}
                  onSetLabel={this.handleSetLabel}
                  onSetScale={this.handleSetScale}
                  onToggleStaticLegend={this.handleToggleStaticLegend}
                  staticLegend={staticLegend}
                  queryConfigs={queriesWorkingDraft}
                  selectedGraphType={cellWorkingType}
                  onSetPrefixSuffix={this.handleSetPrefixSuffix}
                  onSetSuffix={this.handleSetSuffix}
                  onSelectGraphType={this.handleSelectGraphType}
                  onSetYAxisBoundMin={this.handleSetYAxisBoundMin}
                  onSetYAxisBoundMax={this.handleSetYAxisBoundMax}
                />
              : <QueryMaker
                  source={this.getSource()}
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
                  initialGroupByTime={AUTO_GROUP_BY}
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
  sources: arrayOf(shape()),
}

CEOBottom.propTypes = {
  children: node,
}

export default CellEditorOverlay
