import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

const Purgatory = ({currentOrganization}) => {
  return (
    <div>
      <div className="auth-page">
        <div className="auth-box">
          <div className="auth-logo" />
          <div className="auth--purgatory">
            <h3>
              Logged in to <strong>{currentOrganization.name}</strong>
            </h3>
            <p>
              You have not been assigned a Role yet<br />Contact your
              Administrator for assistance
            </p>
          </div>
        </div>
        <p className="auth-credits">
          Made by <span className="icon cubo-uniform" />InfluxData
        </p>
        <div className="auth-image" />
      </div>
    </div>
  )
}

const {shape, string} = PropTypes

Purgatory.propTypes = {
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
}

const mapStateToProps = ({auth: {me: {currentOrganization}}}) => ({
  currentOrganization,
})

export default connect(mapStateToProps)(Purgatory)
