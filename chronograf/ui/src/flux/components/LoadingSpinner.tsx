import React, {SFC, CSSProperties} from 'react'

interface Props {
  style?: CSSProperties
}

const LoadingSpinner: SFC<Props> = ({style}) => {
  return (
    <div className="loading-spinner" style={style}>
      <div className="spinner">
        <div className="bounce1" />
        <div className="bounce2" />
        <div className="bounce3" />
      </div>
    </div>
  )
}

export default LoadingSpinner
