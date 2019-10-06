// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Button} from '@influxdata/clockface'
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import OverlayLink from 'src/overlays/components/OverlayLink'

const DeleteDataButton: FunctionComponent<{}> = () => {
  return (
    <FeatureFlag name="deleteWithPredicate">
      <OverlayLink overlayID="delete-data">
        {onClick => (
          <Button
            text="Delete Data"
            onClick={onClick}
            titleText="Filter and mark data for deletion"
          />
        )}
      </OverlayLink>
      
    </FeatureFlag>
  )
}

export default DeleteDataButton
