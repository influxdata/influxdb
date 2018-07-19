import React, {SFC} from 'react'

import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

interface Props {
  onShowOverlay: () => void
  overlay: JSX.Element
}

const EmptyFluxPage: SFC<Props> = ({onShowOverlay, overlay}) => (
  <div className="page">
    <PageHeader titleText="Flux Editor" fullWidth={true} />
    <div className="page-contents">
      <div className="flux-empty">
        <p>You do not have a configured Flux source</p>
        <button className="btn btn-primary btn-md" onClick={onShowOverlay}>
          Connect to Flux
        </button>
      </div>
    </div>
    {overlay}
  </div>
)

export default EmptyFluxPage
