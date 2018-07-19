import React, {SFC, MouseEvent, CSSProperties} from 'react'
import _ from 'lodash'

const handleClick = (e: MouseEvent<HTMLDivElement>): void => {
  e.stopPropagation()
}

const randomSize = (): CSSProperties => {
  const width = _.random(60, 200)

  return {width: `${width}px`}
}

const LoaderSkeleton: SFC = () => {
  return (
    <>
      <div
        className="flux-schema-tree flux-schema--child"
        onClick={handleClick}
      >
        <div className="flux-schema--item no-hover">
          <div className="flux-schema--expander" />
          <div className="flux-schema--item-skeleton" style={randomSize()} />
        </div>
      </div>
      <div className="flux-schema-tree flux-schema--child">
        <div className="flux-schema--item no-hover">
          <div className="flux-schema--expander" />
          <div className="flux-schema--item-skeleton" style={randomSize()} />
        </div>
      </div>
      <div className="flux-schema-tree flux-schema--child">
        <div className="flux-schema--item no-hover">
          <div className="flux-schema--expander" />
          <div className="flux-schema--item-skeleton" style={randomSize()} />
        </div>
      </div>
    </>
  )
}

export default LoaderSkeleton
