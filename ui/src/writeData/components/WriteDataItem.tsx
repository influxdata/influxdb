// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {SelectableCard, SquareGrid, ComponentSize} from '@influxdata/clockface'

// Utils
import {getOrg} from 'src/organizations/selectors'

// Graphics
import placeholderLogo from 'src/writeData/graphics/placeholderLogo.svg'

// Types
import {WriteDataItem} from 'src/writeData/constants'
import {AppState} from 'src/types'

// Constants
import {ORGS} from 'src/shared/constants/routes'

// Styles
import 'src/writeData/components/WriteDataItem.scss'

interface StateProps {
  orgID: string
}

type Props = WriteDataItem & RouteComponentProps & StateProps

const WriteDataItem: FC<Props> = ({id, name, url, image, history, orgID}) => {
  const handleClick = (): void => {
    history.push(`/${ORGS}/${orgID}/load-data/${url}`)
  }

  let thumb = <img src={placeholderLogo} />

  if (image) {
    thumb = <img src={image} />
  }

  return (
    <SquareGrid.Card key={id}>
      <SelectableCard
        id={id}
        formName="load-data-cards"
        label={name}
        testID={`load-data-item ${id}`}
        selected={false}
        onClick={handleClick}
        fontSize={ComponentSize.ExtraSmall}
        className="write-data--item"
      >
        <div className="write-data--item-thumb">{thumb}</div>
      </SelectableCard>
    </SquareGrid.Card>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)
  return {orgID: id}
}

export default connect<StateProps>(mstp)(withRouter(WriteDataItem))
