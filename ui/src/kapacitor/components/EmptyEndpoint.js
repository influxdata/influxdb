import React from 'react'

const EmptyEndpoint = () => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <div className="form-group">
          This endpoint has not been configured, visit your kapacitor
          configuration page to set it up!
        </div>
      </div>
    </div>
  )
}

export default EmptyEndpoint
