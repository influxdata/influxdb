import React, {PureComponent} from 'react'
import {getDataForCSV} from 'src/data_explorer/apis'
import RadioButtons, {
  RadioButton,
} from 'src/reusable_ui/components/radio_buttons/RadioButtons'
import {OnToggleView} from 'src/data_explorer/components/VisHeaderTab'
import {Source} from 'src/types'

interface Props {
  source: Source
  views: string[]
  view: string
  query: any
  onToggleView: OnToggleView
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

  private handleChangeVisType = (visType: RadioButton) => {
    const {onToggleView} = this.props
    const {text} = visType

    onToggleView(text)
  }

  private get visTypeToggle(): JSX.Element {
    const {views, view} = this.props

    const buttons = views.map(v => ({text: v}))

    const activeButton = {text: view}

    if (views.length) {
      return (
        <RadioButtons
          buttons={buttons}
          activeButton={activeButton}
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
