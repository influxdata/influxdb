// Libraries
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Button} from '@influxdata/clockface'

const DeleteDataButton: FunctionComponent<WithRouterProps> = ({
  location: {pathname},
  router,
}) => {
  const onClick = () => router.push(`${pathname}/delete-data`)

  return (
    <Button
      testID="delete-data-predicate"
      text="Delete Data"
      onClick={onClick}
      titleText="Filter and mark data for deletion"
    />
  )
}

export default withRouter<{}>(DeleteDataButton)
