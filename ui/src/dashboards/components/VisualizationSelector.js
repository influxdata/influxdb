import React from 'react'

const VisualizationSelector = () => (
  <div className="" style={{
    display: 'flex',
    width: '100%',
    background: '#676978',
    padding: '10px',
    borderRadius: '3px',
    marginBottom: '10px',
  }}>
    <div className="">
      VISUALIZATIONS
      <div className="btn btn-info" style={{margin: "0 5px 0 5px"}}>
        Line Graph
      </div>
      <div className="btn btn-info" style={{margin: "0 5px 0 5px"}}>
        SingleStat
      </div>
    </div>
  </div>
)

export default VisualizationSelector
