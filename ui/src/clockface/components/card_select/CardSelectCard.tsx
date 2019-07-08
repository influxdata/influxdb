// Libraries
import React, {PureComponent, StatelessComponent} from 'react'
import classnames from 'classnames'
import {ErrorHandling} from 'src/shared/decorators/errors'
import ProtoboardIcon from 'src/clockface/components/card_select/ProtoboardIcon'

interface Props {
  id: string
  label: string
  onClick: () => void
  name?: string
  image?: StatelessComponent
  hideImage?: boolean
  checked: boolean
  disabled: boolean
  testID?: string
}

@ErrorHandling
class CardSelectCard extends PureComponent<Props> {
  public static defaultProps = {
    checked: false,
    disabled: false,
  }

  public render() {
    const {id, label, checked, name, disabled, testID} = this.props

    return (
      <div
        data-toggle="card_toggle"
        onClick={this.handleClick}
        data-testid={testID}
        className={classnames('card-select--card', {
          'card-select--checked': checked,
          'card-select--disabled': disabled,
          'card-select--active': !disabled,
          'card-select--no-image': this.props.hideImage,
        })}
      >
        <label className="card-select--container">
          <input
            id={`card_select_${id}`}
            name={name}
            type="checkbox"
            value={id}
            defaultChecked={checked}
            disabled={disabled}
          />
          <span
            className={classnames(
              'card-select--checkmark',
              'icon',
              'checkmark',
              {
                'card-select--checked': checked,
              }
            )}
          />
          <div className="card-select--image">{this.cardImage}</div>
          <span className="card-select--label">{label}</span>
        </label>
      </div>
    )
  }

  private get cardImage(): JSX.Element {
    const {image, label, hideImage} = this.props

    if (hideImage) {
      return
    }

    if (image) {
      return React.createElement(image)
    }

    return <ProtoboardIcon displayText={label} />
  }

  private handleClick = e => {
    const {onClick, disabled} = this.props
    e.preventDefault()
    if (!disabled) {
      onClick()
    }
  }
}

export default CardSelectCard
