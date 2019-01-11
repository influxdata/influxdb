import Dygraph from 'dygraphs/src/dygraph'
/* tslint:disable */
/**
 * Synchronize zooming and/or selections between a set of dygraphs.
 *
 * Usage:
 *
 *   var g1 = new Dygraph(...),
 *       g2 = new Dygraph(...),
 *       ...;
 *   var sync = Dygraph.synchronize(g1, g2, ...);
 *   // graphs are now synchronized
 *   sync.detach();
 *   // graphs are no longer synchronized
 *
 * You can set options using the last parameter, for example:
 *
 *   var sync = Dygraph.synchronize(g1, g2, g3, {
 *      selection: true,
 *      zoom: true
 *   });
 *
 * The default is to synchronize both of these.
 *
 * Instead of passing one Dygraph object as each parameter, you may also pass an
 * array of dygraphs:
 *
 *   var sync = Dygraph.synchronize([g1, g2, g3], {
 *      selection: false,
 *      zoom: true
 *   });
 *
 * You may also set `range: false` if you wish to only sync the x-axis.
 * The `range` option has no effect unless `zoom` is true (the default).
 */

var synchronize = function(/* dygraphs..., opts */) {
  if (arguments.length === 0) {
    throw 'Invalid invocation of Dygraph.synchronize(). Need >= 1 argument.'
  }

  var OPTIONS = ['selection', 'zoom', 'range']
  var opts = {
    selection: true,
    zoom: true,
    range: true,
  }
  var dygraphs = []
  var prevCallbacks = []

  var parseOpts = function(obj) {
    if (!(obj instanceof Object)) {
      throw 'Last argument must be either Dygraph or Object.'
    } else {
      for (var i = 0; i < OPTIONS.length; i++) {
        let optName = OPTIONS[i]
        if (obj.hasOwnProperty(optName)) {
          opts[optName] = obj[optName]
        }
      }
    }
  }

  if (arguments[0] instanceof Dygraph) {
    // Arguments are Dygraph objects.
    for (var i = 0; i < arguments.length; i++) {
      if (arguments[i] instanceof Dygraph) {
        dygraphs.push(arguments[i])
      } else {
        break
      }
    }
    if (i < arguments.length - 1) {
      throw new Error(
        'Invalid invocation of Dygraph.synchronize(). ' +
          'All but the last argument must be Dygraph objects.'
      )
    } else if (i === arguments.length - 1) {
      parseOpts(arguments[arguments.length - 1])
    }
  } else if (arguments[0].length) {
    // Invoked w/ list of dygraphs, options
    for (let i = 0; i < arguments[0].length; i++) {
      dygraphs.push(arguments[0][i])
    }
    if (arguments.length === 2) {
      parseOpts(arguments[1])
    } else if (arguments.length > 2) {
      throw new Error(
        'Invalid invocation of Dygraph.synchronize(). ' +
          'Expected two arguments: array and optional options argument.'
      )
    } // otherwise arguments.length == 1, which is fine.
  } else {
    throw new Error(
      'Invalid invocation of Dygraph.synchronize(). ' +
        'First parameter must be either Dygraph or list of Dygraphs.'
    )
  }

  if (dygraphs.length < 2) {
    throw new Error(
      'Invalid invocation of Dygraph.synchronize(). ' +
        'Need two or more dygraphs to synchronize.'
    )
  }

  let readycount = dygraphs.length
  for (let i = 0; i < dygraphs.length; i++) {
    const g = dygraphs[i]
    g.ready(function() {
      if (--readycount === 0) {
        // store original callbacks
        const callBackTypes = [
          'drawCallback',
          'highlightCallback',
          'unhighlightCallback',
        ]
        for (let j = 0; j < dygraphs.length; j++) {
          if (!prevCallbacks[j]) {
            prevCallbacks[j] = {}
          }
          for (let k = callBackTypes.length - 1; k >= 0; k--) {
            prevCallbacks[j][callBackTypes[k]] = dygraphs[j].getFunctionOption(
              callBackTypes[k]
            )
          }
        }

        // Listen for draw, highlight, unhighlight callbacks.
        if (opts.zoom) {
          attachZoomHandlers(dygraphs, opts, prevCallbacks)
        }

        if (opts.selection) {
          attachSelectionHandlers(dygraphs, prevCallbacks)
        }
      }
    })
  }

  return {
    detach() {
      for (let i = 0; i < dygraphs.length; i++) {
        const g = dygraphs[i]
        if (opts.zoom) {
          g.updateOptions({drawCallback: prevCallbacks[i].drawCallback})
        }
        if (opts.selection) {
          g.updateOptions({
            highlightCallback: prevCallbacks[i].highlightCallback,
            unhighlightCallback: prevCallbacks[i].unhighlightCallback,
          })
        }
      }
      // release references & make subsequent calls throw.
      dygraphs = null
      opts = null
      prevCallbacks = null
    },
  }
}

function arraysAreEqual(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) {
    return false
  }
  let i = a.length
  if (i !== b.length) {
    return false
  }
  while (i--) {
    if (a[i] !== b[i]) {
      return false
    }
  }
  return true
}

function attachZoomHandlers(gs, syncOpts, prevCallbacks) {
  let block = false
  for (let i = 0; i < gs.length; i++) {
    const g = gs[i]
    g.updateOptions(
      {
        drawCallback(me, initial) {
          if (block || initial) {
            return
          }
          block = true
          // In the original code, the following assignment was originally
          // var opts = {dateWindow: me.xAxisRange()}, but this assumed that
          // all graphs shared the same time range and thus enforced that.
          type AnyObj = {[x: string]: any}
          const opts = {} as AnyObj
          if (syncOpts.range) {
            opts.valueRange = me.yAxisRange()
          }

          for (let j = 0; j < gs.length; j++) {
            if (gs[j] === me) {
              if (prevCallbacks[j] && prevCallbacks[j].drawCallback) {
                prevCallbacks[j].drawCallback.apply(this, arguments)
              }
              continue
            }

            // Only redraw if there are new options
            if (
              arraysAreEqual(opts.dateWindow, gs[j].getOption('dateWindow')) &&
              arraysAreEqual(opts.valueRange, gs[j].getOption('valueRange'))
            ) {
              continue
            }

            gs[j].updateOptions(opts)
          }
          block = false
        },
      },
      true /* no need to redraw */
    )
  }
}

function attachSelectionHandlers(gs, prevCallbacks) {
  let block = false
  for (let i = 0; i < gs.length; i++) {
    const g = gs[i]

    g.updateOptions(
      {
        highlightCallback(__, x, ___, ____, seriesName) {
          if (block) {
            return
          }
          block = true
          const me = this
          for (let i = 0; i < gs.length; i++) {
            if (me === gs[i]) {
              if (prevCallbacks[i] && prevCallbacks[i].highlightCallback) {
                prevCallbacks[i].highlightCallback.apply(this, arguments)
              }
              continue
            }
            const idx = gs[i].getRowForX(x)
            if (idx !== null) {
              gs[i].setSelection(idx, seriesName)
            }
          }
          block = false
        },
        unhighlightCallback() {
          if (block) {
            return
          }
          block = true
          const me = this
          for (let i = 0; i < gs.length; i++) {
            if (me === gs[i]) {
              if (prevCallbacks[i] && prevCallbacks[i].unhighlightCallback) {
                prevCallbacks[i].unhighlightCallback.apply(this, arguments)
              }
              continue
            }
            gs[i].clearSelection()
          }
          block = false
        },
      },
      true /* no need to redraw */
    )
  }
}

function isValidPoint(p, opt_allowNaNY) {
  if (!p) {
    return false
  } // null or undefined object
  if (p.yval === null) {
    return false
  } // missing point
  if (p.x === null || p.x === undefined) {
    return false
  }
  if (p.y === null || p.y === undefined) {
    return false
  }
  if (isNaN(p.x) || (!opt_allowNaNY && isNaN(p.y))) {
    return false
  }
  return true
}

Dygraph.prototype.findClosestPoint = function(domX, domY) {
  if (Dygraph.VERSION !== '2.0.0') {
    console.error(
      `Dygraph version changed to ${Dygraph.VERSION} - re-copy findClosestPoint`
    )
  }
  let minXDist = Infinity
  let minYDist = Infinity
  let xdist, ydist, dx, dy, point, closestPoint, closestSeries, closestRow
  for (let setIdx = this.layout_.points.length - 1; setIdx >= 0; --setIdx) {
    const points = this.layout_.points[setIdx]
    for (let i = 0; i < points.length; ++i) {
      point = points[i]
      if (!isValidPoint(point, null)) {
        continue
      }

      dx = point.canvasx - domX
      dy = point.canvasy - domY

      xdist = dx * dx
      ydist = dy * dy

      if (xdist < minXDist) {
        minXDist = xdist
        minYDist = ydist
        closestRow = point.idx
        closestSeries = setIdx
        closestPoint = point
      } else if (xdist === minXDist && ydist < minYDist) {
        minXDist = xdist
        minYDist = ydist
        closestRow = point.idx
        closestSeries = setIdx
        closestPoint = point
      }
    }
  }
  const name = this.layout_.setNames[closestSeries]
  return {
    row: closestRow,
    seriesName: name,
    point: closestPoint,
  }
}

Dygraph.synchronize = synchronize

Dygraph.Plugins.Crosshair = (function() {
  'use strict'
  /**
   * Creates the crosshair
   *
   * @constructor
   */

  const crosshair = function(opt_options) {
    this.canvas_ = document.createElement('canvas')
    opt_options = opt_options || {}
    this.direction_ = opt_options.direction || null
  }

  crosshair.prototype.toString = function() {
    return 'Crosshair Plugin'
  }

  /**
   * @param {Dygraph} g Graph instance.
   * @return {object.<string, function(ev)>} Mapping of event names to callbacks.
   */
  crosshair.prototype.activate = function(g) {
    g.graphDiv.appendChild(this.canvas_)

    return {
      select: this.select,
      deselect: this.deselect,
    }
  }

  crosshair.prototype.select = function(e) {
    if (this.direction_ === null) {
      return
    }

    const width = e.dygraph.width_
    const height = e.dygraph.height_
    const xLabelPixels = 20

    this.canvas_.width = width
    this.canvas_.height = height - xLabelPixels
    this.canvas_.style.width = width + 'px' // for IE
    this.canvas_.style.height = height - xLabelPixels + 'px' // for IE

    const ctx = this.canvas_.getContext('2d')
    ctx.clearRect(0, 0, width, height)
    ctx.strokeStyle = '#C6CAD3'
    ctx.lineWidth = 1

    // If graphs have different time ranges, it's possible to select a point on
    // one graph that doesn't exist in another, resulting in an exception.
    if (e.dygraph.selPoints_.length) {
      ctx.beginPath()

      const canvasx = Math.floor(e.dygraph.selPoints_[0].canvasx) + 0.5 // crisper rendering

      if (this.direction_ === 'vertical' || this.direction_ === 'both') {
        ctx.moveTo(canvasx, 0)
        ctx.lineTo(canvasx, height)
      }

      if (this.direction_ === 'horizontal' || this.direction_ === 'both') {
        for (let i = 0; i < e.dygraph.selPoints_.length; i++) {
          const canvasy = Math.floor(e.dygraph.selPoints_[i].canvasy) + 0.5 // crisper rendering
          ctx.moveTo(0, canvasy)
          ctx.lineTo(width, canvasy)
        }
      }

      ctx.stroke()
      ctx.closePath()
    }
  }

  crosshair.prototype.deselect = function() {
    const ctx = this.canvas_.getContext('2d')
    ctx.clearRect(0, 0, this.canvas_.width, this.canvas_.height)
  }

  crosshair.prototype.destroy = function() {
    this.canvas_ = null
  }

  return crosshair
})()

export default Dygraph
