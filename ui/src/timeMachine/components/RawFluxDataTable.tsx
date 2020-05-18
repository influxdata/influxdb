// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import memoizeOne from 'memoize-one'
import RawFluxDataGrid from 'src/timeMachine/components/RawFluxDataGrid'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Utils
import {parseFiles} from 'src/timeMachine/utils/rawFluxDataTable'

interface Props {
  files: string[]
  width: number
  height: number
}

interface State {
  scrollLeft: number
  scrollTop: number
}

class RawFluxDataTable extends PureComponent<Props, State> {
  public state = {scrollLeft: 0, scrollTop: 0}

  private parseFiles = memoizeOne(parseFiles)

  public render() {
    const {width, height, files} = this.props
    const {scrollTop, scrollLeft} = this.state
    const {data, maxColumnCount} = this.parseFiles(files)

    const tableWidth = width
    const tableHeight = height

    return (
      <div className="raw-flux-data-table" data-testid="raw-data-table">
        <FancyScrollbar
          style={{
            overflowY: 'hidden',
            width: tableWidth,
            height: tableHeight,
          }}
          autoHide={false}
          scrollTop={scrollTop}
          scrollLeft={scrollLeft}
          setScrollTop={this.onScrollbarsScroll}
        >
          <RawFluxDataGrid
            scrollTop={scrollTop}
            scrollLeft={scrollLeft}
            width={tableWidth}
            height={tableHeight}
            maxColumnCount={maxColumnCount}
            data={data}
            key={files[0]}
          />
        </FancyScrollbar>
      </div>
    )
  }

  private onScrollbarsScroll = (e: MouseEvent<HTMLElement>) => {
    e.preventDefault()
    e.stopPropagation()

    const {scrollTop, scrollLeft} = e.currentTarget

    this.setState({scrollLeft, scrollTop})
  }
}

export default RawFluxDataTable
