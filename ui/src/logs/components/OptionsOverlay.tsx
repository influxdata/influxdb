import React, {Component} from 'react'

import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'

interface Props {
  onDismissOverlay: () => void
}

interface State {
  isImportable: boolean
}

class OptionsOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isImportable: false,
    }
  }

  public render() {
    const {onDismissOverlay} = this.props

    return (
      <Container maxWidth={600}>
        <Heading title="Configure Log Viewer" onDismiss={onDismissOverlay} />
        <Body>
          <p>Swoggle</p>
        </Body>
      </Container>
    )
  }
}

export default OptionsOverlay
