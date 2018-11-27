// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  loading: RemoteDataState
}

class SparkleSpinner extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    loading: RemoteDataState.NotStarted,
  }

  public render() {
    return (
      <div className="sparkle-spinner">
        <svg
          version="1.1"
          id="sparkle_spinner"
          x="0px"
          y="0px"
          viewBox="0 0 250 250"
        >
          <path
            className={`sparkle-spinner--fill-medium sparkle-spinner--delay-3 ${
              this.animationState
            }`}
            d="M250,9.1C250,9.1,250,9.1,250,9.1l-1.6-7c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0l0,0l0,0c0,0,0,0,0,0L241.5,0
	c0,0,0,0,0,0l0,0l0,0c0,0,0,0,0,0l0,0c0,0,0,0,0,0l-5.2,4.9c0,0,0,0,0,0c0,0,0,0,0,0s0,0,0,0l0,0l0,0c0,0,0,0,0,0l1.6,6.9
	c0,0,0,0,0,0l0,0c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0l6.8,2.1c0,0,0,0,0,0s0,0,0,0h0c0,0,0,0,0,0l0,0L250,9.1
	C250,9.2,250,9.2,250,9.1L250,9.1L250,9.1C250,9.2,250,9.1,250,9.1C250,9.1,250,9.1,250,9.1z"
          />
          <path
            className={`sparkle-spinner--fill-dark sparkle-spinner--delay-1 ${
              this.animationState
            }`}
            d="M168.5,187.9C168.5,187.9,168.5,187.9,168.5,187.9l-0.8-3.5c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0l0,0l0,0
	c0,0,0,0,0,0l-3.5-1c0,0,0,0,0,0l0,0l0,0c0,0,0,0,0,0l0,0c0,0,0,0,0,0l-2.6,2.4c0,0,0,0,0,0c0,0,0,0,0,0s0,0,0,0l0,0l0,0
	c0,0,0,0,0,0l0.8,3.4c0,0,0,0,0,0l0,0c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0l3.4,1c0,0,0,0,0,0c0,0,0,0,0,0h0c0,0,0,0,0,0l0,0
	L168.5,187.9C168.5,187.9,168.5,187.9,168.5,187.9L168.5,187.9L168.5,187.9C168.5,187.9,168.5,187.9,168.5,187.9
	C168.5,187.9,168.5,187.9,168.5,187.9z"
          />
          <circle
            className={`sparkle-spinner--fill-light sparkle-spinner--delay-2 ${
              this.animationState
            }`}
            cx="231.5"
            cy="171.2"
            r="3.5"
          />
          <path
            className={this.loadingStatus}
            d="M181.3,136.9l-10.9-47.6c-0.6-2.6-3-5.2-5.6-6l-50-15.4c-0.6-0.2-1.4-0.2-2.1-0.2
	c-2.1,0-4.4,0.9-5.8,2.1L71,103.1c-2,1.7-3,5.2-2.4,7.7l11.7,51c0.6,2.6,3,5.2,5.6,6l46.7,14.4c0.6,0.2,1.4,0.2,2.1,0.2
	c2.1,0,4.4-0.9,5.8-2.1l38.3-35.6C180.9,142.8,181.9,139.5,181.3,136.9z M120,79.2l34.3,10.6c1.4,0.4,1.4,1,0,1.4l-18,4.1
	c-1.4,0.4-3.2-0.2-4.2-1.2l-12.6-13.6C118.4,79.3,118.7,78.8,120,79.2z M141.4,140.7c0.4,1.4-0.5,2.1-1.9,1.7l-37-11.4
	c-1.4-0.4-1.6-1.5-0.6-2.5l28.3-26.4c1-1,2.1-0.6,2.5,0.7L141.4,140.7z M80.4,107.2l30.1-28c1-1,2.6-0.9,3.6,0.1l15,16.3
	c1,1,0.9,2.6-0.1,3.6l-30.1,28c-1,1-2.6,0.9-3.6-0.1l-15-16.3C79.3,109.6,79.4,108,80.4,107.2z M87.7,151.5l-8-34.9
	c-0.4-1.4,0.2-1.6,1.1-0.6l12.6,13.6c1,1,1.4,3,1,4.4l-5.5,17.7C88.6,152.9,88,152.9,87.7,151.5z M131.7,171.9l-39.3-12.1
	c-1.4-0.4-2.1-1.9-1.7-3.2l6.6-21.1c0.4-1.4,1.9-2.1,3.2-1.7l39.3,12.1c1.4,0.4,2.1,1.9,1.7,3.2l-6.6,21.1
	C134.4,171.6,133.1,172.3,131.7,171.9z M166.5,143.3l-26.2,24.4c-1,1-1.5,0.6-1.1-0.7l5.5-17.7c0.4-1.4,1.9-2.7,3.2-3l18-4.1
	C167.3,141.8,167.5,142.5,166.5,143.3z M169.4,138.1l-21.6,5c-1.4,0.4-2.7-0.5-3.1-1.9l-9.2-40c-0.4-1.4,0.5-2.7,1.9-3.1l21.6-5
	c1.4-0.4,2.7,0.5,3.1,1.9l9.2,40C171.6,136.5,170.7,137.9,169.4,138.1z"
          />
          <g>
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-medium sparkle-spinner--delay-2 ${
                this.animationState
              }`}
              x1="30"
              y1="90.4"
              x2="30"
              y2="104.4"
            />
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-medium sparkle-spinner--delay-2 ${
                this.animationState
              }`}
              x1="37"
              y1="97.4"
              x2="23"
              y2="97.4"
            />
          </g>
          <g>
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-light sparkle-spinner--delay-2 ${
                this.animationState
              }`}
              x1="165.7"
              y1="12.9"
              x2="165.7"
              y2="19.9"
            />
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-light sparkle-spinner--delay-2 ${
                this.animationState
              }`}
              x1="169.2"
              y1="16.4"
              x2="162.2"
              y2="16.4"
            />
          </g>
          <g>
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-dark sparkle-spinner--delay-3 ${
                this.animationState
              }`}
              x1="5.1"
              y1="241.4"
              x2="5.1"
              y2="248.4"
            />
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-dark sparkle-spinner--delay-3 ${
                this.animationState
              }`}
              x1="8.6"
              y1="244.9"
              x2="1.6"
              y2="244.9"
            />
          </g>
          <g>
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-light sparkle-spinner--delay-3 ${
                this.animationState
              }`}
              x1="176.5"
              y1="224.2"
              x2="171.5"
              y2="229.2"
            />
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-light sparkle-spinner--delay-3 ${
                this.animationState
              }`}
              x1="176.5"
              y1="229.2"
              x2="171.5"
              y2="224.2"
            />
          </g>
          <g>
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-light sparkle-spinner--delay-3 ${
                this.animationState
              }`}
              x1="63.4"
              y1="28.3"
              x2="58.4"
              y2="33.3"
            />
            <line
              className={`sparkle-spinner--stroke sparkle-spinner--stroke-light sparkle-spinner--delay-3 ${
                this.animationState
              }`}
              x1="63.4"
              y1="33.3"
              x2="58.4"
              y2="28.3"
            />
          </g>
          <path
            className={`sparkle-spinner--fill-dark sparkle-spinner--delay-1 ${
              this.animationState
            }`}
            d="M101.6,51.6c1.2,0,2.1,1,2.1,2.1s-1,2.1-2.1,2.1s-2.1-1-2.1-2.1S100.5,51.6,101.6,51.6 M101.6,50.2
		c-1.9,0-3.5,1.6-3.5,3.5c0,1.9,1.6,3.5,3.5,3.5c1.9,0,3.5-1.6,3.5-3.5C105.1,51.8,103.6,50.2,101.6,50.2L101.6,50.2z"
          />
          <path
            className={`sparkle-spinner--fill-dark sparkle-spinner--delay-2 ${
              this.animationState
            }`}
            d="M64.2,215.2c3.1,0,5.6,2.5,5.6,5.6s-2.5,5.6-5.6,5.6c-3.1,0-5.6-2.5-5.6-5.6S61.1,215.2,64.2,215.2
		 M64.2,213.9c-3.9,0-7,3.1-7,7s3.1,7,7,7c3.9,0,7-3.1,7-7S68.1,213.9,64.2,213.9L64.2,213.9z"
          />
          <path
            className={`sparkle-spinner--fill-dark sparkle-spinner--delay-1 ${
              this.animationState
            }`}
            d="M38.8,153.1l8.8,2.7l2.1,9l-6.8,6.3l-8.8-2.7l-2.1-9L38.8,153.1 M38.5,151.6C38.5,151.6,38.5,151.6,38.5,151.6
		c-0.1,0-0.1,0-0.1,0c0,0,0,0,0,0l-7.8,7.3c0,0,0,0,0,0c0,0,0,0,0,0s0,0,0,0s0,0,0,0l2.4,10.4c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0
		c0,0,0,0,0,0l10.2,3.1c0,0,0,0,0,0c0,0,0,0,0,0h0c0,0,0,0,0,0l7.8-7.3c0,0,0,0,0,0c0,0,0,0,0-0.1c0,0,0,0,0,0l-2.4-10.4
		c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0L38.5,151.6L38.5,151.6z M51.2,165.3L51.2,165.3L51.2,165.3z M51.2,165.3
		C51.2,165.3,51.2,165.3,51.2,165.3C51.2,165.3,51.2,165.3,51.2,165.3L51.2,165.3z"
          />
          <path
            className={`sparkle-spinner--fill-medium sparkle-spinner--delay-1 ${
              this.animationState
            }`}
            d="M207.5,72.5l7.2,2.2l1.7,7.3l-5.5,5.1l-7.2-2.2l-1.7-7.3L207.5,72.5 M207.3,71C207.2,71,207.2,71,207.3,71
		C207.2,71,207.2,71,207.3,71c-0.1,0-0.1,0-0.1,0l-6.5,6.1c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0l2,8.7c0,0,0,0,0,0
		c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0l8.6,2.6c0,0,0,0,0,0c0,0,0,0,0,0h0c0,0,0,0,0,0l6.5-6.1c0,0,0,0,0,0c0,0,0,0,0-0.1
		c0,0,0,0,0,0l-2-8.7c0,0,0,0,0,0c0,0,0,0,0,0c0,0,0,0,0,0s0,0,0,0L207.3,71L207.3,71z M217.9,82.5L217.9,82.5L217.9,82.5z
		 M217.9,82.5C217.9,82.5,217.9,82.5,217.9,82.5C217.9,82.5,217.9,82.5,217.9,82.5L217.9,82.5z"
          />
        </svg>
      </div>
    )
  }

  private get loadingStatus(): string {
    const {loading} = this.props
    return classnames('sparkle-spinner--logo', {
      'sparkle-spinner--fill-medium':
        loading === RemoteDataState.NotStarted ||
        loading === RemoteDataState.Loading,
      'sparkle-spinner--animate-sparkle': loading === RemoteDataState.Loading,
      'sparkle-spinner--fill-error': loading === RemoteDataState.Error,
      'sparkle-spinner--fill-done': loading === RemoteDataState.Done,
    })
  }

  private get animationState(): string {
    const {loading} = this.props
    return classnames({
      'sparkle-spinner--animate-sparkle': loading === RemoteDataState.Loading,
    })
  }
}

export default SparkleSpinner
