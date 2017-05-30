import React, {PropTypes} from 'react'

import {OVERLAY_CLASS} from 'shared/constants'

const OverlayTechnologies = ({children}) => (
  <div className={OVERLAY_CLASS}>{children}</div>
)

const {node} = PropTypes

OverlayTechnologies.propTypes = {
  children: node.isRequired,
}

export default OverlayTechnologies
