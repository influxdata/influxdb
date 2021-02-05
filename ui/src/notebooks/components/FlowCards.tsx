import React, {useContext} from 'react'

import {ResourceList, Grid, Columns} from '@influxdata/clockface'
import {NotebookListContext} from 'src/notebooks/context/notebook.list'
import FlowsIndexEmpty from 'src/notebooks/components/FlowsIndexEmpty'

import FlowCard from 'src/notebooks/components/FlowCard'

const FlowCards = () => {
  const {notebooks} = useContext(NotebookListContext)
  return (
    <Grid>
      <Grid.Row>
        <Grid.Column
          widthXS={Columns.Twelve}
          widthSM={Columns.Eight}
          widthMD={Columns.Ten}
        >
          <ResourceList>
            <ResourceList.Body emptyState={<FlowsIndexEmpty />}>
              {Object.entries(notebooks).map(([id, {name}]) => {
                return <FlowCard key={id} id={id} name={name} />
              })}
            </ResourceList.Body>
          </ResourceList>
        </Grid.Column>
      </Grid.Row>
    </Grid>
  )
}

export default FlowCards
