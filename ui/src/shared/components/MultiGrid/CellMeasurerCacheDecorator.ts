import {CellMeasurerCache} from 'react-virtualized'

interface CellMeasurerCacheDecoratorParams {
  cellMeasurerCache: CellMeasurerCache
  columnIndexOffset: number
  rowIndexOffset: number
}

interface IndexParam {
  index: number
}

class CellMeasurerCacheDecorator {
  private cellMeasurerCache: CellMeasurerCache
  private columnIndexOffset: number
  private rowIndexOffset: number

  constructor(params: Partial<CellMeasurerCacheDecoratorParams> = {}) {
    const {
      cellMeasurerCache,
      columnIndexOffset = 0,
      rowIndexOffset = 0,
    } = params

    this.cellMeasurerCache = cellMeasurerCache
    this.columnIndexOffset = columnIndexOffset
    this.rowIndexOffset = rowIndexOffset
  }

  public clear(rowIndex: number, columnIndex: number): void {
    this.cellMeasurerCache.clear(
      rowIndex + this.rowIndexOffset,
      columnIndex + this.columnIndexOffset
    )
  }

  public clearAll(): void {
    this.cellMeasurerCache.clearAll()
  }

  public columnWidth = ({index}: IndexParam) => {
    this.cellMeasurerCache.columnWidth({
      index: index + this.columnIndexOffset,
    })
  }

  get defaultHeight(): number {
    return this.cellMeasurerCache.defaultHeight
  }

  get defaultWidth(): number {
    return this.cellMeasurerCache.defaultWidth
  }

  public hasFixedHeight(): boolean {
    return this.cellMeasurerCache.hasFixedHeight()
  }

  public hasFixedWidth(): boolean {
    return this.cellMeasurerCache.hasFixedWidth()
  }

  public getHeight(rowIndex: number, columnIndex: number = 0): number | null {
    return this.cellMeasurerCache.getHeight(
      rowIndex + this.rowIndexOffset,
      columnIndex + this.columnIndexOffset
    )
  }

  public getWidth(rowIndex: number, columnIndex: number = 0): number | null {
    return this.cellMeasurerCache.getWidth(
      rowIndex + this.rowIndexOffset,
      columnIndex + this.columnIndexOffset
    )
  }

  public has(rowIndex: number, columnIndex: number = 0): boolean {
    return this.cellMeasurerCache.has(
      rowIndex + this.rowIndexOffset,
      columnIndex + this.columnIndexOffset
    )
  }

  public rowHeight = ({index}: IndexParam) => {
    this.cellMeasurerCache.rowHeight({
      index: index + this.rowIndexOffset,
    })
  }

  public set(
    rowIndex: number,
    columnIndex: number,
    width: number,
    height: number
  ): void {
    this.cellMeasurerCache.set(
      rowIndex + this.rowIndexOffset,
      columnIndex + this.columnIndexOffset,
      width,
      height
    )
  }
}

export default CellMeasurerCacheDecorator
