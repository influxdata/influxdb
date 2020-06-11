import React, {FC} from 'react'
import {PipeProp} from 'src/notebooks'

const Dance: FC<PipeProp> = ({Context}) => {
  return (
    <Context>
      <div className="notebook-dance">
        <iframe
          width="800"
          height="400"
          src="https://www.youtube.com/embed/nBHkIWAJitg?autoplay=1&loop=1"
          frameborder="0"
          allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
          allowfullscreen
        />
      </div>
    </Context>
  )
}

export default Dance
