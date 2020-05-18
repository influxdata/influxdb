// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'

interface Props {}

const NotebookMinimap: FC<Props> = () => {
  const {id, pipes} = useContext(NotebookContext)

  return (
    <div className="notebook-minimap">
      <div className="notebook-minimap--header">Minimap</div>
      {pipes.map((_, index) => (
        <div key={`pipe-${id}-${index}`} className="notebook-minimap--item">
          {`pipe${index}`}
        </div>
      ))}
    </div>
  )
}

export default NotebookMinimap
