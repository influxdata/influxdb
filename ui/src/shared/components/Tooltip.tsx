import React, {SFC, ReactElement} from 'react'
import ReactTooltip from 'react-tooltip'

interface Props {
  tip: string
  children: ReactElement<any>
}
const Tooltip: SFC<Props> = ({tip, children}) => (
  <div>
    <div data-tip={tip}>{children}</div>
    <ReactTooltip
      effect="solid"
      html={true}
      place="bottom"
      class="influx-tooltip"
    />
  </div>
)

export default Tooltip
