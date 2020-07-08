import React, {FC, useMemo, useContext} from 'react'
import {PipeContext} from 'src/notebooks/context/pipe'

interface Props {
  visible: boolean
}

const Embedded: FC<Props> = ({visible}) => {
  const {data} = useContext(PipeContext)
  const parts = data.uri.split(':')

  if (!visible) {
    return null
  }

  return useMemo(
    () => (
      <iframe
        src={`https://open.spotify.com/embed/${parts[1]}/${parts[2]}`}
        width="600"
        height="80"
        frameBorder="0"
        allow="encrypted-media"
      />
    ),
    [data.uri]
  )
}

export default Embedded
