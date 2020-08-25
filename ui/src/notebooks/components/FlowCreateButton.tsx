// Libraries
import React, {useContext} from 'react'
import {useHistory, useParams} from 'react-router-dom'

// Components
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'
import {NotebookListContext} from 'src/notebooks/context/notebook.list'

const FlowCreateButton = () => {
  const history = useHistory()
  const {orgID} = useParams()
  const {add} = useContext(NotebookListContext)

  const handleCreate = async () => {
    const id = await add()
    history.push(`/orgs/${orgID}/notebooks/${id}`)
  }

  return (
    <Button
      icon={IconFont.Plus}
      color={ComponentColor.Primary}
      text="Create Flow"
      titleText="Click to create a Flow"
      onClick={handleCreate}
      testID="create-flow--button"
    />
  )
}

export default FlowCreateButton
