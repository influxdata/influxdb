import React, {FC, useMemo} from 'react'

interface Props {
  uri: string
  visible: boolean
}

const Embedded: FC<Props> = ({uri, visible}) => {
  const parts = uri.split(':')

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
    [uri]
  )
}

export default Embedded
