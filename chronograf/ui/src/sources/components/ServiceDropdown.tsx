import React, {PureComponent} from 'react'
import {Link, withRouter, RouteComponentProps} from 'react-router'

import Dropdown from 'src/shared/components/Dropdown'

import {Source, Service} from 'src/types'
import {SetActiveService} from 'src/shared/actions/services'
import {getDeep} from 'src/utils/wrappers'

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
        <Link
          to={`/sources/${source.id}/services/new`}
          className="btn btn-xs btn-default"
        >
          <span className="icon plus" /> Add Service Connection
        </Link>
      )
    }

    return (
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
    const service = this.props.services.find(s => {
      return getDeep<boolean>(s, 'metadata.active', false)
    })
    return service || this.props.services[0]
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
