// Libraries
import React, {FC, useState} from 'react'

// Components
import Embedded from './embedded'
import Editor from './editor'
import {SquareButton, IconFont} from '@influxdata/clockface'

// Types
import {PipeProp} from 'src/notebooks'

const Spotify: FC<PipeProp> = ({Context, data, onUpdate}) => {
  const [isEditing, setIsEditing] = useState<boolean>(false)
  const toggleEdit = () => {
    setIsEditing(!isEditing)
  }

  const controls = (
    <SquareButton
      icon={IconFont.CogThick}
      titleText="Edit Spotify URI"
      onClick={toggleEdit}
    />
  )

  const showEditing = isEditing || !data.uri

  return (
    <Context controls={controls}>
      <div className="notebook-spotify">
        <Editor data={data} onUpdate={onUpdate} visible={showEditing} />
        <Embedded uri={data.uri} visible={!showEditing} />
      </div>
    </Context>
  )
}

export default Spotify
