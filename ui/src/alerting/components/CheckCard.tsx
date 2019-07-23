// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {ResourceList} from 'src/clockface'

// Constants
import {DEFAULT_CHECK_NAME} from 'src/alerting/constants'

// Types
import {Check} from 'src/types'

interface Props {
  check: Check
}

const CheckCard: FunctionComponent<Props> = ({check}) => {
  return (
    <ResourceList.Card
      key={`check-id--${check.id}`}
      testID="check-card"
      name={() => (
        <ResourceList.EditableName
          onUpdate={() => {}}
          onClick={() => {}}
          name={check.name}
          noNameString={DEFAULT_CHECK_NAME}
          parentTestID="check-card--name"
          buttonTestID="check-card--name-button"
          inputTestID="check-card--input"
        />
      )}
      updatedAt={check.updatedAt.toString()}
    />
  )
}

export default CheckCard
