// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import {Input} from 'src/clockface'

interface Props {
  prefix: string
  suffix: string
  onUpdatePrefix: (prefix: string) => void
  onUpdateSuffix: (suffix: string) => void
}

class Affixes extends PureComponent<Props> {
  public render() {
    const {prefix, suffix} = this.props

    return (
      <>
        <FormElement colsXS={6} label="Prefix">
          <Input
            value={prefix}
            onChange={this.handleUpdatePrefix}
            placeholder="%, MPH, etc."
          />
        </FormElement>
        <FormElement colsXS={6} label="Suffix">
          <Input
            value={suffix}
            onChange={this.handleUpdateSuffix}
            placeholder="%, MPH, etc."
          />
        </FormElement>
      </>
    )
  }

  private handleUpdatePrefix = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdatePrefix} = this.props
    const prefix = e.target.value
    onUpdatePrefix(prefix)
  }

  private handleUpdateSuffix = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdateSuffix} = this.props
    const suffix = e.target.value
    onUpdateSuffix(suffix)
  }
}

export default Affixes
