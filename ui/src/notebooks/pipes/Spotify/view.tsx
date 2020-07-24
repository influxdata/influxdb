// Libraries
import React, {FC, useState, useContext} from 'react'

// Components
import Embedded from './embedded'
import Editor from './editor'
import {SquareButton, IconFont} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

// Types
import {PipeProp} from 'src/notebooks'

const Spotify: FC<PipeProp> = ({Context}) => {
  const {data} = useContext(PipeContext)
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
        <Editor visible={showEditing} />
        <Embedded visible={!showEditing} />
      </div>
    </Context>
  )
}

export default Spotify
