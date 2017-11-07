import React from 'react'

const Purgatory = () => {
  return (
    <div>
      <div className="auth-page">
        <div className="auth-box">
          <div className="auth-logo" />
          <div className="auth--purgatory">
            <h3>
              Logged in to <strong>[ORGNAME]</strong>
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

export default Purgatory
