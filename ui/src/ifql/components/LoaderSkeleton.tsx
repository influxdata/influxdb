import React, {SFC, MouseEvent} from 'react'

const handleClick = (e: MouseEvent<HTMLDivElement>): void => {
  e.stopPropagation()
}

const LoaderSkeleton: SFC = () => {
  return (
    <>
      <div className="ifql-schema-tree ifql-tree-node" onClick={handleClick}>
        <div className="ifql-schema-item skeleton">
          <div className="ifql-schema-item-toggle" />
          <div className="ifql-schema-item-skeleton" style={{width: '160px'}} />
        </div>
      </div>
      <div className="ifql-schema-tree ifql-tree-node">
        <div className="ifql-schema-item skeleton">
          <div className="ifql-schema-item-toggle" />
          <div className="ifql-schema-item-skeleton" style={{width: '200px'}} />
        </div>
      </div>
      <div className="ifql-schema-tree ifql-tree-node">
        <div className="ifql-schema-item skeleton">
          <div className="ifql-schema-item-toggle" />
          <div className="ifql-schema-item-skeleton" style={{width: '120px'}} />
        </div>
      </div>
    </>
  )
}

export default LoaderSkeleton
