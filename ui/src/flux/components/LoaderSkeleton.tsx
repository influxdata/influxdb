import React, {SFC, MouseEvent} from 'react'

const handleClick = (e: MouseEvent<HTMLDivElement>): void => {
  e.stopPropagation()
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
          <div
            className="flux-schema--item-skeleton"
            style={{width: '160px'}}
          />
        </div>
      </div>
      <div className="flux-schema-tree flux-schema--child">
        <div className="flux-schema--item no-hover">
          <div className="flux-schema--expander" />
          <div
            className="flux-schema--item-skeleton"
            style={{width: '200px'}}
          />
        </div>
      </div>
      <div className="flux-schema-tree flux-schema--child">
        <div className="flux-schema--item no-hover">
          <div className="flux-schema--expander" />
          <div
            className="flux-schema--item-skeleton"
            style={{width: '120px'}}
          />
        </div>
      </div>
    </>
  )
}

export default LoaderSkeleton
