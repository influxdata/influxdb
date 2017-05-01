import React, {PropTypes} from 'react'

const OverlayTechnologies = ({children}) => (
  <div className="overlay-technology">{children}</div>
)

const {node} = PropTypes

OverlayTechnologies.propTypes = {
  children: node.isRequired,
}

export default OverlayTechnologies
