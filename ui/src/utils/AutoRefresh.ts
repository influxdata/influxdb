type func = () => any

class AutoRefresh {
  public subscribers: func[] = []

  private intervalID: NodeJS.Timer

  public subscribe(fn: func) {
    this.subscribers = [...this.subscribers, fn]
  }

  public unsubscribe(fn: func) {
    this.subscribers = this.subscribers.filter(f => f === fn)
  }

  public poll(refreshMs: number) {
    this.clearInterval()

    if (refreshMs) {
      this.intervalID = setInterval(this.refresh, refreshMs)
    }
  }

  private clearInterval() {
    if (!this.intervalID) {
      return
    }

    clearInterval(this.intervalID)
    this.intervalID = null
  }

  private refresh = () => {
    this.subscribers.forEach(fn => fn())
  }
}

export default new AutoRefresh()
