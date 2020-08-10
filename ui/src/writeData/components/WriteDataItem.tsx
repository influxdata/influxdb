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

  let thumbnailStyle = {backgroundImage: `url(${placeholderLogo})`}

  if (image) {
    // TODO: Won't need this one images are imported correctly
    const filePathIsCorrect = !image.replace(/[/]([\w\d])\w+[.]svg/, '').length

    if (filePathIsCorrect) {
      thumbnailStyle = {backgroundImage: `url(${image})`}
    }
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
        className="write-data--item"
      >
        <div className="write-data--item-thumb" style={thumbnailStyle} />
      </SelectableCard>
    </SquareGrid.Card>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)
  return {orgID: id}
}

export default connect<StateProps>(mstp)(withRouter(WriteDataItem))
