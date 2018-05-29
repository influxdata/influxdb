import React, {PureComponent, ReactChildren} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps} from 'react-router'

import {IFQLPage} from 'src/ifql'
import IFQLOverlay from 'src/ifql/components/IFQLOverlay'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'
import {Source, Service, Notification} from 'src/types'
import {Links} from 'src/types/ifql'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  updateScript as updateScriptAction,
  UpdateScript,
} from 'src/ifql/actions'
import * as a from 'src/shared/actions/overlayTechnology'
import * as b from 'src/shared/actions/services'

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
    const {services, sources, notify, updateScript, links, script} = this.props

    if (!this.props.services.length) {
      return null // put loading spinner here
    }

    return (
      <IFQLPage
        sources={sources}
        services={services}
        links={links}
        script={script}
        notify={notify}
        updateScript={updateScript}
      />
    )
  }

  private overlay() {
    const {showOverlay, services, sources, params} = this.props
    const source = sources.find(s => s.id === params.sourceID)

    if (services.length) {
      return
    }

    showOverlay(
      <OverlayContext.Consumer>
        {({onDismissOverlay}) => (
          <IFQLOverlay
            mode="new"
            source={source}
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
    links: links.ifql,
    script,
    sources,
    services,
  }
}

export default connect(mstp, mdtp)(CheckServices)
