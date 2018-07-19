import React, {PureComponent, ReactElement} from 'react'
import {Link, withRouter, RouteComponentProps} from 'react-router'

import Dropdown from 'src/shared/components/Dropdown'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import {Source, Service} from 'src/types'
import {SetActiveService} from 'src/shared/actions/services'

interface Props {
  source: Source
  services: Service[]
  setActiveService: SetActiveService
  deleteService: (service: Service) => void
}

interface ServiceItem {
  text: string
  resource: string
  service: Service
}

class ServiceDropdown extends PureComponent<
  Props & RouteComponentProps<any, any>
> {
  public render() {
    const {source, router, setActiveService, deleteService} = this.props

    if (this.isServicesEmpty) {
      return (
        <Authorized requiredRole={EDITOR_ROLE}>
          <Link
            to={`/sources/${source.id}/services/new`}
            className="btn btn-xs btn-default"
          >
            <span className="icon plus" /> Add Service Connection
          </Link>
        </Authorized>
      )
    }

    return (
      <Authorized
        requiredRole={EDITOR_ROLE}
        replaceWithIfNotAuthorized={this.UnauthorizedDropdown}
      >
        <Dropdown
          className="dropdown-260"
          buttonColor="btn-primary"
          buttonSize="btn-xs"
          items={this.serviceItems}
          onChoose={setActiveService}
          addNew={{
            url: `/sources/${source.id}/services/new`,
            text: 'Add Service Connection',
          }}
          actions={[
            {
              icon: 'pencil',
              text: 'edit',
              handler: item => {
                router.push(`${item.resource}/edit`)
              },
            },
            {
              icon: 'trash',
              text: 'delete',
              handler: item => {
                deleteService(item.service)
              },
              confirmable: true,
            },
          ]}
          selected={this.selected}
        />
      </Authorized>
    )
  }

  private get UnauthorizedDropdown(): ReactElement<HTMLDivElement> {
    return (
      <div className="source-table--service__view-only">{this.selected}</div>
    )
  }

  private get isServicesEmpty(): boolean {
    const {services} = this.props
    return !services || services.length === 0
  }

  private get serviceItems(): ServiceItem[] {
    const {services, source} = this.props

    return services.map(service => {
      return {
        text: service.name,
        resource: `/sources/${source.id}/services/${service.id}`,
        service,
      }
    })
  }

  private get activeService(): Service {
    return this.props.services.find(s => s.active)
  }

  private get selected(): string {
    let selected = ''
    if (this.activeService) {
      selected = this.activeService.name
    } else {
      selected = this.serviceItems[0].text
    }

    return selected
  }
}

export default withRouter<Props>(ServiceDropdown)
