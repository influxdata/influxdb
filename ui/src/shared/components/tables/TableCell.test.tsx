import React from 'react'
import {render} from 'react-testing-library'
import TableCell from 'src/shared/components/tables/TableCell'
import {TableViewProperties} from 'src/client'
import {PropsMultiGrid} from 'src/shared/components/MultiGrid'

describe('Table Cell', () => {
  it('handles empty dates', () => {
    const props = {
      sortOptions: {
        field: 'name',
        direction: 'up',
      },
      data: '',
      dataType: 'dateTime',
      properties: ({} as any) as TableViewProperties,
      hoveredRowIndex: 100,
      hoveredColumnIndex: 100,
      isTimeVisible: true,
      isVerticalTimeAxis: false,
      isFirstColumnFixed: false,
      onClickFieldName: () => {},
      onHover: () => {},
      resolvedFieldOptions: [],
      timeFormatter: (time: string) => {
        throw new Error('GOTEM')
      },
      columnIndex: 0,
      rowIndex: 0,
      key: 'yolo',
      parent: ({props: false} as any) as React.Component<PropsMultiGrid>,
      style: {},
    }

    expect(() => {
      render(<TableCell {...props} />)
    }).not.toThrow()
  })
})
