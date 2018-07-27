import React, {PureComponent, ReactChildren} from 'react'
import {connect} from 'react-redux'
import {WithRouterProps, withRouter} from 'react-router'

import {FluxPage} from 'src/flux'
import EmptyFluxPage from 'src/flux/components/EmptyFluxPage'

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
  fetchServicesAsync: actions.FetchFluxServicesForSourceAsync
  notify: (message: Notification) => void
  updateScript: UpdateScript
  script: string
  links: Links
}

export class CheckServices extends PureComponent<Props & WithRouterProps> {
  constructor(props: Props & WithRouterProps) {
    super(props)
  }

  public async componentDidMount() {
    const source = this.props.sources.find(
      s => s.id === this.props.params.sourceID
    )

    if (!source) {
      return
    }

    await this.props.fetchServicesAsync(source)
  }

  public render() {
    const {services, notify, updateScript, links, script} = this.props

    if (!this.props.services.length) {
      return <EmptyFluxPage onGoToNewService={this.handleGoToNewFlux} />
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
          onGoToEditFlux={this.handleGoToEditFlux}
        />
      </NotificationContext.Provider>
    )
  }

  private handleGoToNewFlux = () => {
    const {router} = this.props
    const addFluxResource = `/sources/${this.source.id}/flux/new`
    router.push(addFluxResource)
  }

  private handleGoToEditFlux = (service: Service) => {
    const {router} = this.props
    const editFluxResource = `/sources/${this.source.id}/flux/${
      service.id
    }/edit`
    router.push(editFluxResource)
  }

  private get source(): Source {
    const {params, sources} = this.props

    return sources.find(s => s.id === params.sourceID)
  }
}

const mdtp = {
  fetchServicesAsync: actions.fetchFluxServicesForSourceAsync,
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

export default withRouter<Props>(connect(mstp, mdtp)(CheckServices))
