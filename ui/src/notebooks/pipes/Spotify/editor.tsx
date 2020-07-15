import React, {FC, useContext} from 'react'
import {Input} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

interface Props {
  visible: boolean
}

const Editor: FC<Props> = ({visible}) => {
  const {data, update} = useContext(PipeContext)
  const _update = evt => {
    update(evt.target.value)
  }

  if (!visible) {
    return null
  }

  return (
    <div className="notebook-spotify--editor">
      <label>Spotify URI</label>
      <Input value={data.uri} onChange={_update} />
    </div>
  )
}

export default Editor
