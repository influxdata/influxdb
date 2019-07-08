// Libraries
import React, {Component, MouseEvent} from 'react'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  name: string
  onClick?: (e: MouseEvent<HTMLAnchorElement>) => void
  placeholder?: string
  parentTestID: string
  buttonTestID: string
  inputTestID: string
  hrefValue: string
}

interface State {
  loading: RemoteDataState
}

class ResourceName extends Component<Props, State> {
  public static defaultProps = {
    parentTestID: 'resource-name',
    buttonTestID: 'resource-name--button',
    inputTestID: 'resource-name--input',
    hrefValue: '#',
  }

  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.Done,
    }
  }

  public render() {
    const {name, hrefValue, parentTestID} = this.props

    return (
      <div className="resource-name" data-testid={parentTestID}>
        <SpinnerContainer
          loading={this.state.loading}
          spinnerComponent={<TechnoSpinner diameterPixels={20} />}
        >
          <a href={hrefValue} onClick={this.handleClick}>
            <span>{name}</span>
          </a>
        </SpinnerContainer>
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLAnchorElement>) => {
    const {onClick} = this.props
    if (onClick) {
      onClick(e)
    }
  }
}

export default ResourceName
