// Libraries
import React, {PureComponent, createRef, RefObject} from 'react'

// Components
import {
  Popover,
  PopoverPosition,
  PopoverInteraction,
  PopoverType,
} from '@influxdata/clockface'


export default class GraphTips extends PureComponent{
  private triggerRef: RefObject<HTMLSpanElement> = createRef()

  public render() {
    return (
      <>
        <span ref={this.triggerRef}>?</span>
        <Popover
          type={PopoverType.Outline}
          position={PopoverPosition.Below}
          triggerRef={this.triggerRef}
          distanceFromTrigger={8}
          showEvent={PopoverInteraction.Hover}
          hideEvent={PopoverInteraction.Hover}
          contents={() => (
            <span>
              <h1>Graph Tips:</h1>
              <p>
                <code>Click + Drag</code> Zoom in (X or Y)
                <br />
                <code>Shift + Click</code> Pan Graph Window
                <br />
                <code>Double Click</code> Reset Graph Window
              </p>
              <h1>Static Legend Tips:</h1>
              <p>
                <code>Click</code>Focus on single Series
                <br />
                <code>Shift + Click</code> Show/Hide single Series
              </p>
            </span>
          )}
        />
      </>
    )
  }
}
