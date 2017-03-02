import React from 'react'

const Center = ({children}) => (
  <div style={{
    position: 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%)',
  }}>
    {children}
  </div>
)

export default Center
