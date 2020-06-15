import React, {FC} from 'react'
import {Input} from '@influxdata/clockface'

import {PipeData} from 'src/notebooks'

interface Props {
  data: PipeData
  onUpdate: (data: PipeData) => void
  visible: boolean
}

const Editor: FC<Props> = ({data, onUpdate, visible}) => {
  const update = evt => {
    onUpdate(evt.target.value)
  }

  if (!visible) {
    return null
  }

  return (
    <div className="notebook-spotify--editor">
      <label>Spotify URI</label>
      <Input value={data.uri} onChange={update} />
    </div>
  )
}

export default Editor
