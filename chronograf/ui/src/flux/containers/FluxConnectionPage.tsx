import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'

import FluxNew from 'src/flux/components/FluxNew'
import FluxEdit from 'src/flux/components/FluxEdit'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {getService} from 'src/shared/apis'
import {FluxFormMode} from 'src/flux/constants/connection'

import {
  updateServiceAsync,
  createServiceAsync,
  CreateServiceAsync,
  fetchFluxServicesForSourceAsync,
  setActiveServiceAsync,
} from 'src/shared/actions/services'
import {couldNotGetFluxService} from 'src/shared/copy/notifications'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {Service, Source, Notification} from 'src/types'

interface Props {
  source: Source
  services: Service[]
  params: {id: string; sourceID: string}
  router: {push: (url: string) => void}
  notify: (message: Notification) => void
  createService: CreateServiceAsync
  updateService: typeof updateServiceAsync
  setActiveFlux: typeof setActiveServiceAsync
  fetchServicesForSource: typeof fetchFluxServicesForSourceAsync
}

interface State {
  service: Service
  formMode: FluxFormMode
}

@ErrorHandling
export class FluxConnectionPage extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(nextProps: Props, prevState: State) {
    const {
      params: {id},
      services,
    } = nextProps

    if (prevState.formMode === FluxFormMode.NEW && id) {
      const service = services.find(s => {
        return s.id === id
      })
      return {
        ...prevState,
        service,
        formMode: FluxFormMode.EDIT,
      }
    }
    return null
  }

  constructor(props) {
    super(props)

    this.state = {
      service: null,
      formMode: FluxFormMode.NEW,
    }
  }

  public async componentDidMount() {
    const {
      source,
      notify,
      params: {id},
      fetchServicesForSource,
    } = this.props

    let service: Service
    let formMode: FluxFormMode
    if (id) {
      try {
        service = await getService(source.links.services, id)
        formMode = FluxFormMode.EDIT
        this.setState({service, formMode})
      } catch (err) {
        console.error('Could not get Service', err)
        notify(couldNotGetFluxService(id))
      }
    } else {
      formMode = FluxFormMode.NEW
      this.setState({formMode})
    }
    await fetchServicesForSource(source)
  }

  public render() {
    return (
      <div className="page">
        <PageHeader titleText={this.pageTitle} />
        <FancyScrollbar className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-8 col-md-offset-2">
                <div className="panel">
                  <div className="panel-body">{this.form}</div>
                </div>
              </div>
            </div>
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private get form() {
    const {
      source,
      notify,
      createService,
      updateService,
      setActiveFlux,
      services,
      router,
    } = this.props
    const {service, formMode} = this.state

    if (formMode === FluxFormMode.NEW) {
      return (
        <FluxNew
          source={source}
          services={services}
          notify={notify}
          router={router}
          setActiveFlux={setActiveFlux}
          createService={createService}
        />
      )
    }
    return (
      <FluxEdit
        notify={notify}
        service={service}
        services={services}
        updateService={updateService}
      />
    )
  }

  private get pageTitle() {
    const {formMode} = this.state

    if (formMode === FluxFormMode.NEW) {
      return 'Add Flux Connection'
    }
    return 'Edit Flux Connection'
  }
}

const mdtp = {
  notify: notifyAction,
  createService: createServiceAsync,
  updateService: updateServiceAsync,
  setActiveFlux: setActiveServiceAsync,
  fetchServicesForSource: fetchFluxServicesForSourceAsync,
}

const mstp = ({services}) => ({services})

export default connect(mstp, mdtp)(withRouter(FluxConnectionPage))
