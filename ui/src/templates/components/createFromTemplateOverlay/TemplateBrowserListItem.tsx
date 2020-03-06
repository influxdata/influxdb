// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'
import {TemplateSummary} from 'src/types'

// Components
import {Icon, IconFont} from '@influxdata/clockface'

interface Props {
  onClick: (template: TemplateSummary) => void
  template: TemplateSummary
  label: string
  selected: boolean
  testID: string
}

class TemplateBrowser extends PureComponent<Props> {
  public render() {
    const {testID, label} = this.props

    return (
      <div
        className={this.className}
        data-testid={testID}
        onClick={this.handleClick}
      >
        <Icon
          glyph={IconFont.Cube}
          className="import-template-overlay--list-icon"
        />
        <span className="import-template-overlay--list-label">{label}</span>
      </div>
    )
  }

  private get className(): string {
    const {selected} = this.props

    return classnames('import-template-overlay--template', {active: selected})
  }

  private handleClick = (): void => {
    const {onClick, template} = this.props

    onClick(template)
  }
}

export default TemplateBrowser
