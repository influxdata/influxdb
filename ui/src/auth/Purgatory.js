import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

import {MEMBER_ROLE} from 'src/auth/Authorized'

const memberCopy = (
  <p>This role does not grant you sufficient permissions to view Chronograf.</p>
)
const viewerCopy = (
  <p>
    This organization does not have any configured sources<br />and your role
    does not have permission to configure a source.
  </p>
)

const Purgatory = ({name, provider, scheme, currentOrganization, role}) =>
  <div>
    <div className="auth-page">
      <div className="auth-box">
        <div className="auth-logo" />
        <div className="auth--purgatory">
          <h3>
            Logged in to <strong>{currentOrganization.name}</strong> as a{' '}
            <em>{role}</em>.
          </h3>
          {role === MEMBER_ROLE ? memberCopy : viewerCopy}
          <p>Contact your Administrator for assistance.</p>
          <hr />
          <pre>
            <code>
              username: {name}
              <br />
              provider: {provider}
              <br />
              scheme: {scheme}
            </code>
          </pre>
        </div>
      </div>
      <p className="auth-credits">
        Made by <span className="icon cubo-uniform" />InfluxData
      </p>
      <div className="auth-image" />
    </div>
  </div>

const {shape, string} = PropTypes

Purgatory.propTypes = {
  name: string.isRequired,
  provider: string.isRequired,
  scheme: string.isRequired,
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  role: string.isRequired,
}

const mapStateToProps = ({
  auth: {me: {name, provider, scheme, currentOrganization, role}},
}) => ({
  name,
  provider,
  scheme,
  currentOrganization,
  role,
})

export default connect(mapStateToProps)(Purgatory)
