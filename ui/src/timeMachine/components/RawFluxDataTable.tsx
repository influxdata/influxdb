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

const PADDING = 10

class RawFluxDataTable extends PureComponent<Props, State> {
  public state = {scrollLeft: 0, scrollTop: 0}

  private parseFiles = memoizeOne(parseFiles)

  public render() {
    const {width, height, files} = this.props
    const {scrollTop, scrollLeft} = this.state
    const {data, maxColumnCount} = this.parseFiles(files)

    const tableWidth = width - PADDING * 2
    const tableHeight = height - PADDING * 2

    return (
      <div className="raw-flux-data-table" style={{padding: `${PADDING}px`}}>
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
