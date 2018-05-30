import React, {PureComponent, ChangeEvent} from 'react'
import {getDeep} from 'src/utils/wrappers'

import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'

import {Dashboard} from 'src/types'
import {DashboardFile} from 'src/types/dashboard'

interface Props {
  onDismissOverlay: () => void
  onImportDashboard: (dashboard: Dashboard) => void
}

interface State {
  dashboardFromFile: Dashboard
}

class ImportDashboardOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      dashboardFromFile: null,
    }
  }

  public render() {
    const {onDismissOverlay} = this.props

    return (
      <Container maxWidth={800}>
        <Heading title="Import Dashboard" onDismiss={onDismissOverlay} />
        <Body>
          <input type="file" onChange={this.handleChooseFile} accept=".json" />
          <button
            className="btn btn-sm btn-default"
            onClick={this.handleImportDashboard}
          >
            Import
          </button>
        </Body>
      </Container>
    )
  }

  private handleChooseFile = (e: ChangeEvent<HTMLInputElement>): void => {
    const file = e.target.files[0]
    const fileReader = new FileReader()
    fileReader.onloadend = () => {
      const result: DashboardFile = JSON.parse(fileReader.result)
      const dashboard = getDeep<Dashboard>(result, 'dashboard', null)
      this.setState({dashboardFromFile: dashboard})
    }
    fileReader.readAsText(file)
  }

  private handleImportDashboard = (): void => {
    const {onImportDashboard, onDismissOverlay} = this.props
    const {dashboardFromFile} = this.state
    if (dashboardFromFile) {
      onImportDashboard(dashboardFromFile)
    }
    onDismissOverlay()
  }
}

export default ImportDashboardOverlay
