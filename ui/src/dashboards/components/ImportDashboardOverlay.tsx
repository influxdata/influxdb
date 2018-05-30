import React, {Component} from 'react'
import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'

interface Props {
  onDismissOverlay: () => void
}

class ImportDashboardOverlay extends Component<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {onDismissOverlay} = this.props

    return (
      <Container maxWidth={800}>
        <Heading title="Import Dashboard" onDismiss={onDismissOverlay} />
        <Body>
          <p>sweeeet</p>
        </Body>
      </Container>
    )
  }
}

export default ImportDashboardOverlay
