import React, {PureComponent} from 'react'

import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

import {Service} from 'src/types'

interface Props {
  service: Service
  services: Service[]
  onGoToEditFlux: (service: Service) => void
}

class FluxHeader extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    return (
      <>
        <PageHeader
          titleText="Flux Editor"
          fullWidth={true}
          optionsComponents={this.optionsComponents}
        />
      </>
    )
  }

  private get optionsComponents(): JSX.Element {
    return (
      <button
        onClick={this.handleGoToEditFlux}
        className="btn btn-sm btn-default"
      >
        Edit Connection
      </button>
    )
  }

  private handleGoToEditFlux = () => {
    const {service, onGoToEditFlux} = this.props
    onGoToEditFlux(service)
  }
}

export default FluxHeader
