import React, {SFC} from 'react'
import ReactTooltip from 'react-tooltip'

const graphTipsText =
  '<h1>Graph Tips:</h1><p><code>Click + Drag</code> Zoom in (X or Y)<br/><code>Shift + Click</code> Pan Graph Window<br/><code>Double Click</code> Reset Graph Window</p><h1>Static Legend Tips:</h1><p><code>Click</code>Focus on single Series<br/><code>Shift + Click</code> Show/Hide single Series</p>'

const GraphTips: SFC = () => (
  <div
    className="graph-tips"
    data-for="graph-tips-tooltip"
    data-tip={graphTipsText}
  >
    <span>?</span>
    <ReactTooltip
      id="graph-tips-tooltip"
      effect="solid"
      html={true}
      place="bottom"
      class="influx-tooltip"
    />
  </div>
)

export default GraphTips
