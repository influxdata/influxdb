import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

const Tooltip = ({tip, children}) =>
  <div>
    <div data-tip={tip}>
      {children}
    </div>
    <ReactTooltip
      effect="solid"
      html={true}
      offset={{top: 2}}
      place="bottom"
      class="influx-tooltip place-bottom"
    />
  </div>

const {shape, string} = PropTypes

Tooltip.propTypes = {
  tip: string,
  children: shape({}),
}

export default Tooltip
