// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {IconFont, Button, ComponentColor} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

const SaveAsButton: FunctionComponent<{}> = () => {
  return (
    <OverlayLink overlayID="save-as">
      {onClick => (
        <Button
          icon={IconFont.Export}
          text="Save As"
          onClick={onClick}
          color={ComponentColor.Primary}
          titleText="Save your query as a Dashboard Cell or a Task"
        />
      )}
    </OverlayLink>
  )
}

export default SaveAsButton
