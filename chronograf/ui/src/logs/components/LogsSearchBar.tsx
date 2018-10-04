import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

import {Input, IconFont, Button, ComponentColor} from 'src/clockface'
import PointInTimeDropDown from 'src/logs/components/PointInTimeDropDown'

interface Props {
  onSearch: (value: string) => void
  customTime?: string
  relativeTime?: number
  onChooseCustomTime: (time: string) => void
  onChooseRelativeTime: (time: number) => void
}

interface State {
  searchTerm: string
}

class LogsSearchBar extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
    // TODO: fix search bar styling
    const {
      customTime,
      relativeTime,
      onChooseCustomTime,
      onChooseRelativeTime,
    } = this.props

    return (
      <div className="logs-viewer--search-bar">
        <div className="logs-viewer--search-input">
          <Input
            icon={IconFont.Search}
            placeholder="Search logs using keywords or regular expressions..."
            onChange={this.handleChange}
            onKeyDown={this.handleInputKeyDown}
            value={searchTerm}
          />
        </div>
        <PointInTimeDropDown
          customTime={customTime}
          relativeTime={relativeTime}
          onChooseCustomTime={onChooseCustomTime}
          onChooseRelativeTime={onChooseRelativeTime}
        />
        <Button
          text="Search"
          color={ComponentColor.Primary}
          onClick={this.handleSearch}
        />
      </div>
    )
  }

  private handleSearch = () => {
    this.props.onSearch(this.state.searchTerm)
    this.setState({searchTerm: ''})
  }

  private handleInputKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      return this.handleSearch()
    }
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}

export default LogsSearchBar
