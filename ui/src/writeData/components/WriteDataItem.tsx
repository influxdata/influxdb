// Libraries
import React, {FC, createElement} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {SelectableCard, SquareGrid, ComponentSize} from '@influxdata/clockface'

// Utils
import {getOrg} from 'src/organizations/selectors'

// Types
import {WriteDataItem} from 'src/writeData/constants'
import {AppState} from 'src/types'

// Constants
import {ORGS} from 'src/shared/constants/routes'

interface StateProps {
  orgID: string
}

type Props = WriteDataItem & RouteComponentProps & StateProps

const WriteDataItem: FC<Props> = ({id, name, url, image, history, orgID}) => {
  const handleClick = (): void => {
    history.push(`/${ORGS}/${orgID}/load-data/${url}`)
  }

  let cardBody = <>{name}</>

  if (image) {
    cardBody = createElement(image)
  }

  return (
    <SquareGrid.Card key={id}>
      <SelectableCard
        id={id}
        formName="client-libraries-cards"
        label={name}
        testID={`client-libraries-cards--${id}`}
        selected={false}
        onClick={handleClick}
        fontSize={ComponentSize.ExtraSmall}
      >
        {cardBody}
      </SelectableCard>
    </SquareGrid.Card>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)
  return {orgID: id}
}

export default connect<StateProps>(mstp)(withRouter(WriteDataItem))
