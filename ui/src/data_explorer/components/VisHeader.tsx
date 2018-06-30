import React, {PureComponent} from 'react'
import {getDataForCSV} from 'src/data_explorer/apis'
import RadioButtons from 'src/reusable_ui/components/radio_buttons/RadioButtons'
import {Source} from 'src/types'

interface Props {
  source: Source
  views: string[]
  view: string
  query: any
  onToggleView: (view: string) => void
  errorThrown: () => void
}

class VisHeader extends PureComponent<Props> {
  public render() {
    return (
      <div className="graph-heading">
        {this.visTypeToggle}
        {this.downloadButton}
      </div>
    )
  }

  private handleChangeVisType = (view: string) => {
    const {onToggleView} = this.props

    onToggleView(view)
  }

  private get visTypeToggle(): JSX.Element {
    const {views, view} = this.props

    if (views.length) {
      return (
        <RadioButtons
          buttons={views}
          activeButton={view}
          onChange={this.handleChangeVisType}
        />
      )
    }

    return null
  }

  private get downloadButton(): JSX.Element {
    const {query, source, errorThrown} = this.props

    if (query) {
      return (
        <div
          className="btn btn-sm btn-default dlcsv"
          onClick={getDataForCSV(source, query, errorThrown)}
        >
          <span className="icon download dlcsv" />
          .CSV
        </div>
      )
    }

    return null
  }
}

export default VisHeader
