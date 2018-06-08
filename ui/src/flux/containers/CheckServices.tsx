import React, {PureComponent, ReactChildren} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps} from 'react-router'

import {FluxPage} from 'src/flux'
import FluxOverlay from 'src/flux/components/FluxOverlay'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'
import {Source, Service, Notification} from 'src/types'
import {Links} from 'src/types/flux'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  updateScript as updateScriptAction,
  UpdateScript,
} from 'src/flux/actions'
import * as a from 'src/shared/actions/overlayTechnology'
import * as b from 'src/shared/actions/services'

export const NotificationContext = React.createContext()

const actions = {...a, ...b}

interface Props {
  sources: Source[]
  services: Service[]
  children: ReactChildren
  showOverlay: a.ShowOverlay
  fetchServicesAsync: b.FetchServicesAsync
  notify: (message: Notification) => void
  updateScript: UpdateScript
  script: string
  links: Links
}

export class CheckServices extends PureComponent<Props & WithRouterProps> {
  public async componentDidMount() {
    const source = this.props.sources.find(
      s => s.id === this.props.params.sourceID
    )

    if (!source) {
      return
    }

    await this.props.fetchServicesAsync(source)

    if (!this.props.services.length) {
      this.overlay()
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
      </NotificationContext.Provider>
    )
  }

  private get source(): Source {
    const {params, sources} = this.props

    return sources.find(s => s.id === params.sourceID)
  }

  private overlay() {
    const {showOverlay, services} = this.props

    if (services.length) {
      return
    }

    showOverlay(
      <OverlayContext.Consumer>
        {({onDismissOverlay}) => (
          <FluxOverlay
            mode="new"
            source={this.source}
            onDismiss={onDismissOverlay}
          />
        )}
      </OverlayContext.Consumer>,
      {}
    )
  }
}

const mdtp = {
  fetchServicesAsync: actions.fetchServicesAsync,
  showOverlay: actions.showOverlay,
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
