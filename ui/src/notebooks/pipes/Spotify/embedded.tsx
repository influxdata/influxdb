import React, {FC, useMemo} from 'react'

interface Props {
  uri: string
  visible: boolean
}

const Embedded: FC<Props> = ({uri, visible}) => {
  const parts = uri.split(':')
  const p1 = parts[1]
  const p2 = parts[2]

  return useMemo(
    () =>
      visible && (
        <iframe
          src={`https://open.spotify.com/embed/${p1}/${p2}`}
          width="600"
          height="80"
          frameBorder="0"
          allow="encrypted-media"
        />
      ),
    [visible, p1, p2]
  )
}

export default Embedded
