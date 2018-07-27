import React, {SFC} from 'react'

import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

interface Props {
  onGoToNewService: () => void
}

const EmptyFluxPage: SFC<Props> = ({onGoToNewService}) => (
  <div className="page">
    <PageHeader titleText="Flux Editor" fullWidth={true} />
    <div className="page-contents">
      <div className="flux-empty">
        <p>You do not have a configured Flux source</p>
        <button className="btn btn-primary btn-md" onClick={onGoToNewService}>
          Connect to Flux
        </button>
      </div>
    </div>
  </div>
)

export default EmptyFluxPage
