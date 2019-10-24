// // Libraries
// import React, {SFC, useRef, RefObject} from 'react'
// import ReactTooltip from 'react-tooltip'

// // Components
// import {Popover, PopoverPosition, PopoverType,ButtonRef} from '@influxdata/clockface'

// const graphTipsText =
//   '<h1>Graph Tips:</h1><p><code>Click + Drag</code> Zoom in (X or Y)<br/><code>Shift + Click</code> Pan Graph Window<br/><code>Double Click</code> Reset Graph Window</p><h1>Static Legend Tips:</h1><p><code>Click</code>Focus on single Series<br/><code>Shift + Click</code> Show/Hide single Series</p>'
// const triggerRef: RefObject<ButtonRef> = useRef(null)

// const GraphTips: SFC = () => (
//   <div
//   className="graph-tips"
//   data-for="graph-tips-tooltip"
//   data-tip={graphTipsText}
// >
//   <span>?</span>
//   <Popover
//   type={PopoverType.Outline}
//   position={PopoverPosition.ToTheRight}
//   triggerRef={triggerRef}
//   distanceFromTrigger={8}
//   contents={()=> (
//     graphTipsText
//   )}
// />
// </div>
  
  
// )

// export default GraphTips
