// Libraries
import React, {Component} from 'react'
import _ from 'lodash'
import uuid from 'uuid'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ResizeContainer from 'src/shared/components/ResizeContainer'
import QueryMaker from 'src/dashboards/components/QueryMaker'
import Visualization from 'src/dashboards/components/Visualization'
import OverlayControls from 'src/dashboards/components/OverlayControls'
import DisplayOptions from 'src/dashboards/components/DisplayOptions'
import CEOBottom from 'src/dashboards/components/CEOBottom'

// APIs
import {getQueryConfigAndStatus} from 'src/shared/apis'

// Utils
import {getDeep} from 'src/utils/wrappers'
import * as queryTransitions from 'src/utils/queryTransitions'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {buildQuery} from 'src/utils/influxql'
import {nextSource} from 'src/dashboards/utils/sources'

// Constants
import {IS_STATIC_LEGEND} from 'src/shared/constants'
import {TYPE_QUERY_CONFIG} from 'src/dashboards/constants'
import {removeUnselectedTemplateValues} from 'src/tempVars/constants'
import {OVERLAY_TECHNOLOGY} from 'src/shared/constants/classNames'
import {MINIMUM_HEIGHTS, INITIAL_HEIGHTS} from 'src/data_explorer/constants'
import {
  AUTO_GROUP_BY,
  PREDEFINED_TEMP_VARS,
  TEMP_VAR_DASHBOARD_TIME,
} from 'src/shared/constants'
import {getCellTypeColors} from 'src/dashboards/constants/cellEditor'

// Types
import * as Types from 'src/types/modules'

type QueryTransitions = typeof queryTransitions
type EditRawTextAsyncFunc = (
  url: string,
  id: string,
  text: string
) => Promise<void>
type CellEditorOverlayActionsFunc = (queryID: string, ...args: any[]) => void
type QueryActions = {
  [K in keyof QueryTransitions]: CellEditorOverlayActionsFunc
}
export type CellEditorOverlayActions = QueryActions & {
  editRawTextAsync: EditRawTextAsyncFunc
}

const staticLegend: Types.Dashboards.Data.Legend = {
  type: 'static',
  orientation: 'bottom',
}

interface Template {
  tempVar: string
}

interface QueryStatus {
  queryID: string
  status: Types.Queries.Data.Status
}

interface Props {
  sources: Types.Sources.Data.Source[]
  editQueryStatus: Types.Dashboards.Actions.EditCellQueryStatusActionCreator
  onCancel: () => void
  onSave: (cell: Types.Dashboards.Data.Cell) => void
  source: Types.Sources.Data.Source
  dashboardID: number
  queryStatus: QueryStatus
  autoRefresh: number
  templates: Template[]
  timeRange: Types.Queries.Data.TimeRange
  thresholdsListType: string
  thresholdsListColors: Types.Colors.Data.ColorNumber[]
  gaugeColors: Types.Colors.Data.ColorNumber[]
  lineColors: Types.Colors.Data.ColorString[]
  cell: Types.Dashboards.Data.Cell
}

interface State {
  queriesWorkingDraft: Types.Queries.Data.QueryConfig[]
  activeQueryIndex: number
  isDisplayOptionsTabActive: boolean
  isStaticLegend: boolean
}

const createWorkingDraft = (
  source: Types.Sources.Data.Source,
  query: Types.Dashboards.Data.CellQuery
): Types.Queries.Data.QueryConfig => {
  const {queryConfig} = query
  const draft: Types.Queries.Data.QueryConfig = {
    ...queryConfig,
    id: uuid.v4(),
    source,
  }

  return draft
}

const createWorkingDrafts = (
  source: Types.Sources.Data.Source,
  queries: Types.Dashboards.Data.CellQuery[]
): Types.Queries.Data.QueryConfig[] =>
  _.cloneDeep(
    queries.map((query: Types.Dashboards.Data.CellQuery) =>
      createWorkingDraft(source, query)
    )
  )

@ErrorHandling
class CellEditorOverlay extends Component<Props, State> {
  private overlayRef: HTMLDivElement

  constructor(props) {
    super(props)

    const {
      cell: {legend},
    } = props
    let {
      cell: {queries},
    } = props

    // Always have at least one query
    if (_.isEmpty(queries)) {
      queries = [{id: uuid.v4()}]
    }

    const queriesWorkingDraft = createWorkingDrafts(this.initialSource, queries)

    this.state = {
      queriesWorkingDraft,
      activeQueryIndex: 0,
      isDisplayOptionsTabActive: false,
      isStaticLegend: IS_STATIC_LEGEND(legend),
    }
  }

  public componentWillReceiveProps(nextProps: Props) {
    const {status, queryID} = this.props.queryStatus
    const {queriesWorkingDraft} = this.state
    const {queryStatus} = nextProps

    if (
      queryStatus.status &&
      queryStatus.queryID &&
      (queryStatus.queryID !== queryID || queryStatus.status !== status)
    ) {
      const nextQueries = queriesWorkingDraft.map(
        q => (q.id === queryID ? {...q, status: queryStatus.status} : q)
      )
      this.setState({queriesWorkingDraft: nextQueries})
    }
  }

  public componentDidMount() {
    if (this.overlayRef) {
      this.overlayRef.focus()
    }
  }

  public render() {
    const {
      onCancel,
      templates,
      timeRange,
      autoRefresh,
      editQueryStatus,
    } = this.props

    const {
      activeQueryIndex,
      isDisplayOptionsTabActive,
      queriesWorkingDraft,
      isStaticLegend,
    } = this.state

    return (
      <div
        className={OVERLAY_TECHNOLOGY}
        onKeyDown={this.handleKeyDown}
        tabIndex={0}
        ref={this.onRef}
      >
        <ResizeContainer
          containerClass="resizer--full-size"
          minTopHeight={MINIMUM_HEIGHTS.visualization}
          minBottomHeight={MINIMUM_HEIGHTS.queryMaker}
          initialTopHeight={INITIAL_HEIGHTS.visualization}
          initialBottomHeight={INITIAL_HEIGHTS.queryMaker}
        >
          <Visualization
            timeRange={timeRange}
            templates={templates}
            autoRefresh={autoRefresh}
            queryConfigs={queriesWorkingDraft}
            editQueryStatus={editQueryStatus}
            staticLegend={isStaticLegend}
            isInCEO={true}
          />
          <CEOBottom>
            <OverlayControls
              onCancel={onCancel}
              queries={queriesWorkingDraft}
              sources={this.formattedSources}
              onSave={this.handleSaveCell}
              selected={this.findSelectedSource()}
              onSetQuerySource={this.handleSetQuerySource}
              isSavable={this.isSaveable}
              isDisplayOptionsTabActive={isDisplayOptionsTabActive}
              onClickDisplayOptions={this.handleClickDisplayOptionsTab}
            />
            {isDisplayOptionsTabActive ? (
              <DisplayOptions
                queryConfigs={queriesWorkingDraft}
                onToggleStaticLegend={this.handleToggleStaticLegend}
                staticLegend={isStaticLegend}
                onResetFocus={this.handleResetFocus}
              />
            ) : (
              <QueryMaker
                source={this.source}
                templates={templates}
                queries={queriesWorkingDraft}
                actions={this.queryActions}
                timeRange={timeRange}
                onDeleteQuery={this.handleDeleteQuery}
                onAddQuery={this.handleAddQuery}
                activeQueryIndex={activeQueryIndex}
                activeQuery={this.getActiveQuery()}
                setActiveQueryIndex={this.handleSetActiveQueryIndex}
                initialGroupByTime={AUTO_GROUP_BY}
              />
            )}
          </CEOBottom>
        </ResizeContainer>
      </div>
    )
  }

  private get formattedSources(): Types.Sources.Data.SourceOption[] {
    const {sources} = this.props
    return sources.map(s => ({
      ...s,
      text: `${s.name} @ ${s.url}`,
    }))
  }

  private onRef = (r: HTMLDivElement) => {
    this.overlayRef = r
  }

  private queryStateReducer = (
    queryTransition
  ): CellEditorOverlayActionsFunc => (queryID: string, ...payload: any[]) => {
    const {queriesWorkingDraft} = this.state
    const queryWorkingDraft = queriesWorkingDraft.find(q => q.id === queryID)

    const nextQuery = queryTransition(queryWorkingDraft, ...payload)

    const nextQueries = queriesWorkingDraft.map(q => {
      if (q.id === queryWorkingDraft.id) {
        return {...nextQuery, source: nextSource(q, nextQuery)}
      }

      return q
    })

    this.setState({queriesWorkingDraft: nextQueries})
  }

  private handleAddQuery = () => {
    const {queriesWorkingDraft} = this.state
    const newIndex = queriesWorkingDraft.length

    this.setState({
      queriesWorkingDraft: [
        ...queriesWorkingDraft,
        {...defaultQueryConfig({id: uuid.v4()}), source: this.initialSource},
      ],
    })
    this.handleSetActiveQueryIndex(newIndex)
  }

  private handleDeleteQuery = index => {
    const {queriesWorkingDraft} = this.state
    const nextQueries = queriesWorkingDraft.filter((__, i) => i !== index)

    this.setState({queriesWorkingDraft: nextQueries})
  }

  private handleSaveCell = () => {
    const {queriesWorkingDraft, isStaticLegend} = this.state
    const {cell, thresholdsListColors, gaugeColors, lineColors} = this.props

    const queries: Types.Dashboards.Data.CellQuery[] = queriesWorkingDraft.map(
      q => {
        const timeRange = q.range || {
          upper: null,
          lower: TEMP_VAR_DASHBOARD_TIME,
        }
        const source = getDeep<string | null>(q.source, 'links.self', null)
        return {
          queryConfig: q,
          query: q.rawText || buildQuery(TYPE_QUERY_CONFIG, timeRange, q),
          source,
        }
      }
    )

    const colors = getCellTypeColors({
      cellType: cell.type,
      gaugeColors,
      thresholdsListColors,
      lineColors,
    })

    const newCell: Types.Dashboards.Data.Cell = {
      ...cell,
      queries,
      colors,
      legend: isStaticLegend ? staticLegend : {},
    }

    this.props.onSave(newCell)
  }

  private handleClickDisplayOptionsTab = isDisplayOptionsTabActive => () => {
    this.setState({isDisplayOptionsTabActive})
  }

  private handleSetActiveQueryIndex = activeQueryIndex => {
    this.setState({activeQueryIndex})
  }

  private handleToggleStaticLegend = isStaticLegend => () => {
    this.setState({isStaticLegend})
  }

  private handleSetQuerySource = (source: Types.Sources.Data.Source): void => {
    const queriesWorkingDraft: Types.Queries.Data.QueryConfig[] = this.state.queriesWorkingDraft.map(
      q => ({
        ..._.cloneDeep(q),
        source,
      })
    )
    this.setState({queriesWorkingDraft})
  }

  private getActiveQuery = () => {
    const {queriesWorkingDraft, activeQueryIndex} = this.state
    const activeQuery = _.get(
      queriesWorkingDraft,
      activeQueryIndex,
      queriesWorkingDraft[0]
    )

    const queryText = _.get(activeQuery, 'rawText', '')
    const userDefinedTempVarsInQuery = this.findUserDefinedTempVarsInQuery(
      queryText,
      this.props.templates
    )

    if (!!userDefinedTempVarsInQuery.length) {
      activeQuery.isQuerySupportedByExplorer = false
    }

    return activeQuery
  }

  private findUserDefinedTempVarsInQuery = (
    query: string,
    templates: Template[]
  ): Template[] => {
    return templates.filter((temp: Template) => {
      if (!query) {
        return false
      }
      const isPredefinedTempVar: boolean = !!PREDEFINED_TEMP_VARS.find(
        t => t === temp.tempVar
      )
      if (!isPredefinedTempVar) {
        return query.includes(temp.tempVar)
      }
      return false
    })
  }

  // The schema explorer is not built to handle user defined template variables
  // in the query in a clear manner. If they are being used, we indicate that in
  // the query config in order to disable the fields column down stream because
  // at this point the query string is disconnected from the schema explorer.
  private handleEditRawText = async (
    url: string,
    id: string,
    text: string
  ): Promise<void> => {
    const userDefinedTempVarsInQuery = this.findUserDefinedTempVarsInQuery(
      text,
      this.props.templates
    )

    const isUsingUserDefinedTempVars: boolean = !!userDefinedTempVarsInQuery.length

    try {
      const selectedTempVars: Template[] = isUsingUserDefinedTempVars
        ? removeUnselectedTemplateValues(userDefinedTempVarsInQuery)
        : []

      const {data} = await getQueryConfigAndStatus(
        url,
        [{query: text, id}],
        selectedTempVars
      )

      const config = data.queries.find(q => q.id === id)
      const nextQueries: Types.Queries.Data.QueryConfig[] = this.state.queriesWorkingDraft.map(
        (q: Types.Queries.Data.QueryConfig) => {
          if (q.id === id) {
            const isQuerySupportedByExplorer = !isUsingUserDefinedTempVars

            if (isUsingUserDefinedTempVars) {
              return {...q, rawText: text, isQuerySupportedByExplorer}
            }

            return {
              ...config.queryConfig,
              source: q.source,
              isQuerySupportedByExplorer,
            }
          }

          return q
        }
      )

      this.setState({queriesWorkingDraft: nextQueries})
    } catch (error) {
      console.error(error)
    }
  }

  private findSelectedSource = (): string => {
    const {source} = this.props
    const sources = this.formattedSources
    const currentSource = getDeep<Types.Sources.Data.Source | null>(
      this.state.queriesWorkingDraft,
      '0.source',
      null
    )

    if (!currentSource) {
      const defaultSource: Types.Sources.Data.Source = sources.find(
        s => s.id === source.id
      )
      return (defaultSource && defaultSource.text) || 'No sources'
    }

    const selected: Types.Sources.Data.Source = sources.find(
      s => s.links.self === currentSource.links.self
    )
    return (selected && selected.text) || 'No sources'
  }

  private handleKeyDown = e => {
    switch (e.key) {
      case 'Enter':
        if (!e.metaKey) {
          return
        } else if (e.target === this.overlayRef) {
          this.handleSaveCell()
        } else {
          e.target.blur()
          setTimeout(this.handleSaveCell, 50)
        }
        break
      case 'Escape':
        if (e.target === this.overlayRef) {
          this.props.onCancel()
        } else {
          const targetIsDropdown = e.target.classList[0] === 'dropdown'
          const targetIsButton = e.target.tagName === 'BUTTON'

          if (targetIsDropdown || targetIsButton) {
            return this.props.onCancel()
          }

          e.target.blur()
          this.overlayRef.focus()
        }
        break
    }
  }

  private handleResetFocus = () => {
    this.overlayRef.focus()
  }

  private get isSaveable(): boolean {
    const {queriesWorkingDraft} = this.state

    return queriesWorkingDraft.every(
      (query: Types.Queries.Data.QueryConfig) =>
        (!!query.measurement && !!query.database && !!query.fields.length) ||
        !!query.rawText
    )
  }

  private get queryActions(): CellEditorOverlayActions {
    const mapped: QueryActions = _.mapValues<
      QueryActions,
      CellEditorOverlayActionsFunc
    >(queryTransitions, v => this.queryStateReducer(v)) as QueryActions

    const result: CellEditorOverlayActions = {
      ...mapped,
      editRawTextAsync: this.handleEditRawText,
    }

    return result
  }

  private get initialSource(): Types.Sources.Data.Source {
    const {
      cell: {queries},
      source,
      sources,
    } = this.props

    const initialSourceLink: string = getDeep<string>(queries, '0.source', null)

    if (initialSourceLink) {
      const initialSource = sources.find(
        s => s.links.self === initialSourceLink
      )

      return initialSource
    }
    return source
  }

  private get source(): Types.Sources.Data.Source {
    const {source, sources} = this.props
    const query = _.get(this.state.queriesWorkingDraft, 0, {source: null})

    if (!query.source) {
      return source
    }

    const foundSource = sources.find(
      s =>
        s.links.self ===
        getDeep<string | null>(query, 'source.links.self', null)
    )
    if (foundSource) {
      return foundSource
    }
    return source
  }
}

export default CellEditorOverlay
