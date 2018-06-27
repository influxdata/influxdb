import React, {PureComponent, ReactChildren} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps} from 'react-router'

import {FluxPage} from 'src/flux'
import FluxOverlay from 'src/flux/components/FluxOverlay'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import {Source, Service, Notification} from 'src/types'
import {Links} from 'src/types/flux'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  updateScript as updateScriptAction,
  UpdateScript,
} from 'src/flux/actions'
import * as actions from 'src/shared/actions/services'

export const NotificationContext = React.createContext()

interface Props {
  sources: Source[]
  services: Service[]
  children: ReactChildren
  fetchServicesAsync: actions.FetchServicesAsync
  notify: (message: Notification) => void
  updateScript: UpdateScript
  script: string
  links: Links
}

interface State {
  showOverlay: boolean
}

export class CheckServices extends PureComponent<
  Props & WithRouterProps,
  State
> {
  constructor(props: Props & WithRouterProps) {
    super(props)

    this.state = {
      showOverlay: false,
    }
  }

  public async componentDidMount() {
    const source = this.props.sources.find(
      s => s.id === this.props.params.sourceID
    )

    if (!source) {
      return
    }

    await this.props.fetchServicesAsync(source)

    if (!this.props.services.length) {
      this.setState({showOverlay: true})
    }
  }

  public render() {
    const {services, notify, updateScript, links, script} = this.props

    if (!this.props.services.length) {
      return null // put loading spinner here
    }

    return (
      <NotificationContext.Provider value={{notify}}>
        <FluxPage
          source={this.source}
          services={services}
          links={links}
          script={script}
          notify={notify}
          updateScript={updateScript}
        />
        {this.renderOverlay}
      </NotificationContext.Provider>
    )
  }

  private get source(): Source {
    const {params, sources} = this.props

    return sources.find(s => s.id === params.sourceID)
  }

  private get renderOverlay(): JSX.Element {
    const {showOverlay} = this.state

    return (
      <OverlayTechnology visible={showOverlay}>
        <FluxOverlay
          mode="new"
          source={this.source}
          onDismiss={this.handleDismissOverlay}
        />
      </OverlayTechnology>
    )
  }

  private handleDismissOverlay = (): void => {
    this.setState({showOverlay: false})
  }
}

const mdtp = {
  fetchServicesAsync: actions.fetchServicesAsync,
  updateScript: updateScriptAction,
  notify: notifyAction,
}

const mstp = ({sources, services, links, script}) => {
  return {
    links: links.flux,
    script,
    sources,
    services,
  }
}

export default connect(mstp, mdtp)(CheckServices)
