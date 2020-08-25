import React, {FC} from 'react'
import {useParams, useHistory} from 'react-router-dom'

// Components
import {ResourceCard} from '@influxdata/clockface'

interface Props {
  id: string
}

const FlowCard: FC<Props> = ({id}) => {
  const {orgID} = useParams()
  const history = useHistory()

  const handleClick = () => {
    history.push(`/orgs/${orgID}/notebooks/${id}`)
  }

  return (
    <ResourceCard key={`flow-card--${id}`}>
      <ResourceCard.Name name={id} onClick={handleClick} />
    </ResourceCard>
  )
}

export default FlowCard
