import React, {PureComponent, ReactChildren} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

import IFQLOverlay from 'src/ifql/components/IFQLOverlay'
import {OverlayContext} from 'src/shared/components/OverlayTechnology'
import {Source, Service} from 'src/types'
import * as a from 'src/shared/actions/overlayTechnology'
import * as b from 'src/shared/actions/services'

const actions = {...a, ...b}

interface Props {
  sources: Source[]
  services: Service[]
  children: ReactChildren
  showOverlay: a.ShowOverlay
  fetchServicesAsync: b.FetchServicesAsync
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
    if (!this.props.services.length) {
      return null
    }

    return this.props.children
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
}

const mstp = ({sources, services}) => ({sources, services})

export default connect(mstp, mdtp)(withRouter(CheckServices))
