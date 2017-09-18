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

    const {cell: {name, type, queries, axes}, sources} = props

    let source = [...sources, ...this.dummySources].find(
      s =>
        s.links.self === _.get(queries, ['0', source], props.source.links.self)
    )
    source = source && source.links.self

    const queriesWorkingDraft = _.cloneDeep(
      queries.map(({queryConfig}) => ({
        ...queryConfig,
        id: uuid.v4(),
        source,
      }))
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

  handleSetQuerySource = source => {
    const queriesWorkingDraft = this.state.queriesWorkingDraft.map(q => ({
      ..._.cloneDeep(q),
      source: source.links.self,
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
        q => (q.id === id ? config.queryConfig : q)
      )
      this.setState({queriesWorkingDraft: nextQueries})
    } catch (error) {
      console.error(error)
    }
  }

  dummySources = () => {
    let dummies = []
    for (let i = 2; i < 13; i++) {
      const id = 1 + i
      dummies = [
        ...dummies,
        {
          id,
          name: `dummyFlux ${i}`,
          type: 'influx',
          username: '',
          url: `http://9X.se3ShredURL02x6tall1sStartURL16enlargedj216stretching1fganglingeexpanded1lankydeep4434longishsustained6EzURLTightURL30URLPieShrinkrwtoweringalankyPiURLr4qSHurl0prolongedloftyprotracted00greatd0Dwarfurl83runningcUrlTea1Redirx30Dwarfurl13a1lnk.in74nu1z106p6011lanky0Shrtndenduring1URl.iebkelongateShredURLi1dlastinglengthenedU7618ShrinkrDecentURLk2longishShrtndelongateds1U76MyURLsSitelutions000toweringShoterLinka1462Fly217ooexpanded1080B6541continuedstretched0U760elongated3Shrtnd301URL4SHurlganglingx07ShoterLinkrangywYATUC1719Is.gdURLvi3ShortURLv0a8stretchingprotractedc2URLcut0ganglingUlimit51DigBig16q3Redirx1l16c0010farawayYATUC0t1stretch0remote91b0bfaraway0elongate59URLvi4lnk.in841SimURLDwarfurlr32towering701Doiop07311a5825lastingz0918TinyLinkenlargedexpanded30CanURLUlimits105d33f0y6172Sitelutions3lengthened6longishShredURLk20a17x04ShrinkURLprotractedv3drawn%2Boutm6greatvXilXil58ShortURL9f1d16protracteddistant780151enlargedv6301URLsustainedspread%2Boutl00c38URLPie7gangling8035s08s131f8i0Ulimit0nTinyLink9elongatedlasting908deeplengthy0lanky116TraceURL7ax0m6ShrinkURLo1llastingShrinkURLCanURLm060great1TinyLink0561f19MyURL4370Fly21d2drawn%2Bout0stretchedenduring1113011026cURLvijenduringShortenURL18GetShortyShrinkr56e8y416URL7spread%2Bout7ocURLHawk5cahighDwarfurl1elongate4efcURL.co.ukz73Ulimit1RubyURLi7lnk.inDigBigdeep97protractedg58WapURL0761YepIt77esustainedremote452e461sustained9U76NanoRefjPiURL5enduring0X.seorSitelutions111stringyU76CanURL9Shrtnd9stringyBeam.toemSHurlgreatc0e011hremote162lengthenedstretchlengthy1506U76xrangy0Fly210tb3111eiURLCuttera1stringy5gangling0Shortlinksgangling10loftyc00A2Nd1MyURL1drawn%2Bouty5SmallrtURLCutterDigBig00g8Metamarkrunningre04prolonged4EasyURLfURl.ieUrlTeatoweringSnipURLjbr0191b170wextensiveMooURL1Shorl932U76spun%2Bout6tall2i0DoiopwlankyURLHawkq6EzURL921uURLHawkehcspread%2Bout11g10krunningystretching08oDecentURL16279Xil3stringy1l6321URLcutShrtndeuelongate1010lastingexpanded5ShrinkrMyURLstretching5drawn%2Bout50distant40faraway24elongate7nstretchinggreat7delongated1SnipURL301URL74e69ttallf1A2N31${i}`,
          default: false,
          telegraf: 'telegraf',
          links: {
            self: `/chronograf/v1/sources/${i}`,
          },
        },
      ]
    }

    return dummies
  }

  formatSources = [...this.props.sources, ...this.dummySources()].map(s => ({
    ...s,
    text: `${s.name} @ ${s.url}`,
  }))

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
      axes,
      activeQueryIndex,
      cellWorkingName,
      cellWorkingType,
      isDisplayOptionsTabActive,
      queriesWorkingDraft,
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
                  source={source}
                  sources={this.formatSources}
                  onSetBase={this.handleSetBase}
                  onSetLabel={this.handleSetLabel}
                  onSetScale={this.handleSetScale}
                  queryConfigs={queriesWorkingDraft}
                  selectedGraphType={cellWorkingType}
                  onSetQuerySource={this.handleSetQuerySource}
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
  sources: arrayOf(shape()),
}

CEOBottom.propTypes = {
  children: node,
}

export default CellEditorOverlay
