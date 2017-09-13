import React, {Component, PropTypes} from 'react'

import GraphTypeSelector from 'src/dashboards/components/GraphTypeSelector'
import AxesOptions from 'src/dashboards/components/AxesOptions'
import SourceSelector from 'src/dashboards/components/SourceSelector'

import {buildDefaultYLabel} from 'shared/presenters'

class DisplayOptions extends Component {
  constructor(props) {
    super(props)

    const {axes, queryConfigs} = props

    this.state = {
      axes: this.setDefaultLabels(axes, queryConfigs),
    }
  }

  componentWillReceiveProps(nextProps) {
    const {axes, queryConfigs} = nextProps

    this.setState({axes: this.setDefaultLabels(axes, queryConfigs)})
  }

  setDefaultLabels(axes, queryConfigs) {
    return queryConfigs.length
      ? {
          ...axes,
          y: {...axes.y, defaultYLabel: buildDefaultYLabel(queryConfigs[0])},
        }
      : axes
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

  findSelectedSource = () => {
    const {cellSource, source: defaultSource} = this.props
    const sources = this.formatSources
    const cellSourceID = cellSource && cellSource.id

    let selected
    selected = sources.find(s => s.id === cellSourceID)

    if (selected) {
      return selected.text
    }

    selected = sources.find(s => s.id === defaultSource.id)
    return (selected && selected.text) || 'No sources'
  }

  render() {
    const {
      onSetBase,
      onSetScale,
      onSetLabel,
      onSetCellSource,
      selectedGraphType,
      onSelectGraphType,
      onSetPrefixSuffix,
      onSetYAxisBoundMin,
      onSetYAxisBoundMax,
    } = this.props
    const {axes} = this.state

    return (
      <div className="display-options">
        <div style={{display: 'flex', flexDirection: 'column', flex: 1}}>
          <AxesOptions
            axes={axes}
            onSetBase={onSetBase}
            onSetLabel={onSetLabel}
            onSetScale={onSetScale}
            onSetPrefixSuffix={onSetPrefixSuffix}
            onSetYAxisBoundMin={onSetYAxisBoundMin}
            onSetYAxisBoundMax={onSetYAxisBoundMax}
          />
          <SourceSelector
            sources={this.formatSources}
            onSetCellSource={onSetCellSource}
            selected={this.findSelectedSource()}
          />
        </div>
        <GraphTypeSelector
          selectedGraphType={selectedGraphType}
          onSelectGraphType={onSelectGraphType}
        />
      </div>
    )
  }
}
const {arrayOf, func, shape, string} = PropTypes

DisplayOptions.propTypes = {
  sources: arrayOf(shape()).isRequired,
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
  onSetPrefixSuffix: func.isRequired,
  onSetYAxisBoundMin: func.isRequired,
  onSetYAxisBoundMax: func.isRequired,
  onSetCellSource: func.isRequired,
  onSetScale: func.isRequired,
  onSetLabel: func.isRequired,
  onSetBase: func.isRequired,
  axes: shape({}).isRequired,
  queryConfigs: arrayOf(shape()).isRequired,
  source: shape({}).isRequired,
  cellSource: shape({}),
}

export default DisplayOptions
