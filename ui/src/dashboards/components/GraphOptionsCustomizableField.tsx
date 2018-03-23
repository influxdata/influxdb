import React, {PureComponent} from 'react'

import InputClickToEdit from 'src/shared/components/InputClickToEdit'

interface Field {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  internalName: string
  displayName: string
  visible: boolean
  onFieldUpdate: (field: Field) => void
}

class GraphOptionsCustomizableField extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)

    this.handleFieldRename = this.handleFieldRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }

  public handleFieldRename(rename: string) {
    const {onFieldUpdate, internalName, visible} = this.props
    onFieldUpdate({internalName, displayName: rename, visible})
  }

  public handleToggleVisible() {
    const {onFieldUpdate, internalName, displayName, visible} = this.props
    onFieldUpdate({internalName, displayName, visible: !visible})
  }

  public render() {
    const {internalName, displayName, visible} = this.props

    return (
      <div className="customizable-field">
        <div
          className={
            visible
              ? 'customizable-field--label'
              : 'customizable-field--label__hidden'
          }
          onClick={this.handleToggleVisible}
          title={
            visible
              ? `Click to HIDE ${internalName}`
              : `Click to SHOW ${internalName}`
          }
        >
          <span className={visible ? 'icon eye-open' : 'icon eye-closed'} />
          {internalName}
        </div>
        <InputClickToEdit
          value={displayName}
          wrapperClass="customizable-field--input"
          onBlur={this.handleFieldRename}
          placeholder={`Rename ${internalName}`}
          appearAsNormalInput={true}
          disabled={!visible}
        />
      </div>
    )
  }
}

export default GraphOptionsCustomizableField
